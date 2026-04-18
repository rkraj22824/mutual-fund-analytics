package ingestion

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"mf-analytics/internal/analytics"
	"mf-analytics/internal/config"
	"mf-analytics/internal/database"
	"mf-analytics/internal/models"
)

const (
	maxJobRetries      = 3
	workerPollInterval = 5 * time.Second
	pruneJobsInterval  = 24 * time.Hour
	pruneJobsAge       = 7 * 24 * time.Hour
	minBackfillRecords = 100 // threshold below which we always (re-)backfill
)

// Pipeline orchestrates backfill and incremental NAV ingestion.
// It uses a persistent job queue (sync_jobs table) so all work survives
// process crashes. Jobs stuck in "running" are reset to "pending" on startup.
type Pipeline struct {
	cfg       *config.Config
	db        *database.DB
	client    *Client
	analytics *analytics.Engine
	status    string
}

// NewPipeline constructs a Pipeline.
func NewPipeline(
	cfg *config.Config,
	db *database.DB,
	client *Client,
	analyticsEngine *analytics.Engine,
) *Pipeline {
	return &Pipeline{
		cfg:       cfg,
		db:        db,
		client:    client,
		analytics: analyticsEngine,
		status:    "idle",
	}
}

// Run is the pipeline's main loop. It must be called in its own goroutine.
//
// Startup sequence:
//  1. Recover stale jobs (reset running → pending)
//  2. Seed the scheme registry
//  3. Enqueue backfill jobs for schemes that lack sufficient history
//  4. Enter the polling loop (process jobs + periodic incremental sync)
func (p *Pipeline) Run(ctx context.Context) {
	log.Println("[pipeline] starting")

	// ── Step 1: crash recovery ────────────────────────────────────────────────
	stats, err := RecoverStaleJobs(p.db)
	if err != nil {
		log.Printf("[pipeline] crash recovery: %v", err)
	} else if stats.StaleJobsRecovered > 0 {
		log.Printf("[pipeline] crash recovery: %d stale job(s) reset to pending",
			stats.StaleJobsRecovered)
	}

	// ── Step 2: seed scheme registry ─────────────────────────────────────────
	if err := SeedSchemes(p.db); err != nil {
		log.Printf("[pipeline] seed schemes: %v", err)
	}

	// ── Step 3: enqueue backfills for schemes that need them ──────────────────
	if err := p.enqueueBackfills(); err != nil {
		log.Printf("[pipeline] enqueue backfills: %v", err)
	}

	// ── Step 4: polling loop ──────────────────────────────────────────────────
	jobTicker := time.NewTicker(workerPollInterval)
	defer jobTicker.Stop()

	syncTicker := time.NewTicker(p.cfg.SyncInterval)
	defer syncTicker.Stop()

	pruneTicker := time.NewTicker(pruneJobsInterval)
	defer pruneTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[pipeline] stopped")
			return

		case <-jobTicker.C:
			p.processNextJob(ctx)

		case <-syncTicker.C:
			log.Println("[pipeline] daily sync tick")
			if err := p.enqueueIncrementals(); err != nil {
				log.Printf("[pipeline] enqueue incrementals: %v", err)
			}

		case <-pruneTicker.C:
			if n, err := PruneOldCompletedJobs(p.db, pruneJobsAge); err != nil {
				log.Printf("[pipeline] prune jobs: %v", err)
			} else if n > 0 {
				log.Printf("[pipeline] pruned %d old completed jobs", n)
			}
		}
	}
}

// TriggerSync is called by POST /sync/trigger.
func (p *Pipeline) TriggerSync(_ context.Context) error {
	return p.enqueueIncrementals()
}

// Status returns a snapshot of pipeline and rate-limit state.
func (p *Pipeline) Status() (models.PipelineStatus, error) {
	rows, err := p.db.Query(
		`SELECT status, COUNT(*) FROM sync_jobs GROUP BY status`,
	)
	if err != nil {
		return models.PipelineStatus{}, err
	}
	defer rows.Close()

	counts := map[string]int{}
	for rows.Next() {
		var status string
		var cnt int
		rows.Scan(&status, &cnt)
		counts[status] = cnt
	}
	if err := rows.Err(); err != nil {
		return models.PipelineStatus{}, err
	}

	var lastSyncRaw sql.NullString
	p.db.QueryRow(
		`SELECT value FROM pipeline_state WHERE key = 'last_incremental_at'`,
	).Scan(&lastSyncRaw)

	var lastSync *time.Time
	if lastSyncRaw.Valid {
		if t, err := time.Parse(time.RFC3339, lastSyncRaw.String); err == nil {
			lastSync = &t
		}
	}

	return models.PipelineStatus{
		Status:     p.status,
		Total:      len(targetSchemes),
		Completed:  counts[models.JobCompleted],
		InProgress: counts[models.JobRunning],
		Failed:     counts[models.JobFailed],
		Pending:    counts[models.JobPending],
		RateLimit:  p.client.rateLimiter.Status(),
		LastSyncAt: lastSync,
		Message: fmt.Sprintf("queue: %d pending, %d running, %d failed",
			counts[models.JobPending],
			counts[models.JobRunning],
			counts[models.JobFailed]),
	}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Job queue management
// ─────────────────────────────────────────────────────────────────────────────

func (p *Pipeline) enqueueBackfills() error {
	schemes, err := LoadSchemesFromDB(p.db, "", "")
	if err != nil {
		return err
	}

	queued := 0
	for _, s := range schemes {
		// Skip schemes that already have a pending/running/completed backfill
		// AND have fresh enough data.
		var activeCnt int
		p.db.QueryRow(`
			SELECT COUNT(*) FROM sync_jobs
			WHERE scheme_code = ? AND job_type = 'backfill'
			  AND status IN ('pending', 'running', 'completed')`,
			s.Code,
		).Scan(&activeCnt)

		if activeCnt > 0 {
			// Still skip if the data is fresh; otherwise allow re-backfill.
			skip, _ := ShouldSkipBackfill(p.db, s.Code, minBackfillRecords)
			if skip {
				continue
			}
		}

		// Use ShouldSkipBackfill as the primary guard.
		skip, err := ShouldSkipBackfill(p.db, s.Code, minBackfillRecords)
		if err != nil {
			log.Printf("[pipeline] skip check %s: %v", s.Code, err)
		}
		if skip {
			continue
		}

		if err := p.enqueueJob(s.Code, models.JobTypeBackfill); err != nil {
			log.Printf("[pipeline] enqueue backfill %s: %v", s.Code, err)
			continue
		}
		queued++
	}

	if queued > 0 {
		log.Printf("[pipeline] enqueued %d backfill job(s)", queued)
	}
	return nil
}

func (p *Pipeline) enqueueIncrementals() error {
	schemes, err := LoadSchemesFromDB(p.db, "", "")
	if err != nil {
		return err
	}
	for _, s := range schemes {
		if err := p.enqueueJob(s.Code, models.JobTypeIncremental); err != nil {
			log.Printf("[pipeline] enqueue incremental %s: %v", s.Code, err)
		}
	}
	log.Printf("[pipeline] enqueued %d incremental job(s)", len(schemes))
	return nil
}

// enqueueJob inserts a job only if no pending/running job already exists
// for the same scheme+type (idempotent).
func (p *Pipeline) enqueueJob(schemeCode, jobType string) error {
	var cnt int
	p.db.QueryRow(`
		SELECT COUNT(*) FROM sync_jobs
		WHERE scheme_code = ? AND job_type = ? AND status IN ('pending', 'running')`,
		schemeCode, jobType,
	).Scan(&cnt)
	if cnt > 0 {
		return nil
	}
	_, err := p.db.Exec(`
		INSERT INTO sync_jobs (scheme_code, job_type, status)
		VALUES (?, ?, 'pending')`,
		schemeCode, jobType,
	)
	return err
}

// ─────────────────────────────────────────────────────────────────────────────
// Job execution
// ─────────────────────────────────────────────────────────────────────────────

func (p *Pipeline) processNextJob(ctx context.Context) {
	job, err := p.claimNextJob()
	if err != nil || job == nil {
		return
	}

	p.status = "running"
	log.Printf("[pipeline] job id=%d type=%s scheme=%s (retry=%d)",
		job.ID, job.JobType, job.SchemeCode, job.RetryCount)

	var execErr error
	switch job.JobType {
	case models.JobTypeBackfill:
		execErr = p.runBackfill(ctx, job)
	case models.JobTypeIncremental:
		execErr = p.runIncremental(ctx, job)
	default:
		execErr = fmt.Errorf("unknown job type: %s", job.JobType)
	}

	if execErr != nil {
		log.Printf("[pipeline] job %d failed (attempt %d): %v",
			job.ID, job.RetryCount+1, execErr)
		p.failJob(job, execErr.Error())
	} else {
		p.completeJob(job)
		if err := p.analytics.ComputeAll(job.SchemeCode); err != nil {
			log.Printf("[pipeline] analytics compute %s: %v", job.SchemeCode, err)
		}
	}
	p.status = "idle"
}

// claimNextJob atomically picks the oldest pending job and marks it "running".
func (p *Pipeline) claimNextJob() (*models.SyncJob, error) {
	tx, err := p.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var job models.SyncJob
	var createdRaw, updatedRaw string

	err = tx.QueryRow(`
		SELECT id, scheme_code, job_type, status, error_msg, retry_count,
		       created_at, updated_at
		FROM   sync_jobs
		WHERE  status = 'pending'
		  AND  retry_count < ?
		ORDER  BY created_at ASC
		LIMIT  1`,
		maxJobRetries,
	).Scan(
		&job.ID, &job.SchemeCode, &job.JobType, &job.Status,
		&job.ErrorMsg, &job.RetryCount, &createdRaw, &updatedRaw,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	job.CreatedAt, _ = time.Parse(time.RFC3339, createdRaw)
	job.UpdatedAt, _ = time.Parse(time.RFC3339, updatedRaw)

	_, err = tx.Exec(`
		UPDATE sync_jobs SET status = 'running', updated_at = ?
		WHERE id = ?`,
		time.Now().UTC().Format(time.RFC3339), job.ID,
	)
	if err != nil {
		return nil, err
	}
	return &job, tx.Commit()
}

// runBackfill fetches the full NAV history and stores it with checkpointing.
// Because bulkInsertNAV uses ON CONFLICT DO UPDATE, re-running after a crash
// is always safe – we only write records we don't already have or update
// changed values.
func (p *Pipeline) runBackfill(ctx context.Context, job *models.SyncJob) error {
	// Check if we already have fresh data (crash-then-restart within the same day).
	if skip, _ := ShouldSkipBackfill(p.db, job.SchemeCode, minBackfillRecords); skip {
		log.Printf("[pipeline] backfill %s: skipping (already has fresh data)", job.SchemeCode)
		return nil
	}

	points, scheme, err := p.client.FetchSchemeHistory(ctx, job.SchemeCode)
	if err != nil {
		return err
	}
	if err := p.upsertSchemeMetadata(scheme); err != nil {
		return fmt.Errorf("upsert scheme: %w", err)
	}
	if err := p.bulkInsertNAV(points); err != nil {
		return fmt.Errorf("insert nav: %w", err)
	}

	// Persist checkpoint so diagnostics can see progress.
	if len(points) > 0 {
		latest := points[0]
		for _, pt := range points {
			if pt.Date.After(latest.Date) {
				latest = pt
			}
		}
		if err := CheckpointBackfill(p.db, job.SchemeCode, latest.Date, len(points)); err != nil {
			log.Printf("[pipeline] checkpoint %s: %v", job.SchemeCode, err)
		}
	}

	if err := p.markSchemeSynced(job.SchemeCode); err != nil {
		return fmt.Errorf("mark synced: %w", err)
	}

	log.Printf("[pipeline] backfill %s: stored %d NAV records", job.SchemeCode, len(points))
	clearCheckpoint(p.db, job.SchemeCode) // clean up after successful completion
	return nil
}

// runIncremental fetches and upserts the latest NAV only.
func (p *Pipeline) runIncremental(ctx context.Context, job *models.SyncJob) error {
	point, err := p.client.FetchLatestNAV(ctx, job.SchemeCode)
	if err != nil {
		return err
	}
	_, err = p.db.Exec(`
		INSERT INTO nav_records (scheme_code, date, nav)
		VALUES (?, ?, ?)
		ON CONFLICT(scheme_code, date) DO UPDATE SET nav = excluded.nav`,
		point.SchemeCode,
		point.Date.Format("2006-01-02"),
		point.NAV,
	)
	if err != nil {
		return fmt.Errorf("upsert latest nav: %w", err)
	}
	if err := p.markSchemeSynced(job.SchemeCode); err != nil {
		return err
	}
	p.setPipelineState("last_incremental_at", time.Now().UTC().Format(time.RFC3339))
	log.Printf("[pipeline] incremental %s: nav=%.4f date=%s",
		job.SchemeCode, point.NAV, point.Date.Format("2006-01-02"))
	return nil
}

// bulkInsertNAV inserts a batch of NAV records in chunked transactions.
// ON CONFLICT DO UPDATE makes every run idempotent.
func (p *Pipeline) bulkInsertNAV(points []models.NAVPoint) error {
	const chunkSize = 500
	for start := 0; start < len(points); start += chunkSize {
		end := start + chunkSize
		if end > len(points) {
			end = len(points)
		}
		chunk := points[start:end]

		tx, err := p.db.Begin()
		if err != nil {
			return err
		}
		stmt, err := tx.Prepare(`
			INSERT INTO nav_records (scheme_code, date, nav)
			VALUES (?, ?, ?)
			ON CONFLICT(scheme_code, date) DO UPDATE SET nav = excluded.nav`)
		if err != nil {
			tx.Rollback()
			return err
		}
		for _, pt := range chunk {
			if _, err := stmt.Exec(
				pt.SchemeCode, pt.Date.Format("2006-01-02"), pt.NAV,
			); err != nil {
				stmt.Close()
				tx.Rollback()
				return err
			}
		}
		stmt.Close()
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func (p *Pipeline) upsertSchemeMetadata(s *models.Scheme) error {
	if s == nil {
		return nil
	}
	_, err := p.db.Exec(`
		INSERT INTO schemes (code, name, amc, category)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(code) DO UPDATE SET
		    name     = excluded.name,
		    amc      = excluded.amc,
		    category = excluded.category`,
		s.Code, s.Name, s.AMC, s.Category,
	)
	return err
}

func (p *Pipeline) markSchemeSynced(code string) error {
	_, err := p.db.Exec(
		`UPDATE schemes SET last_synced_at = ? WHERE code = ?`,
		time.Now().UTC().Format(time.RFC3339), code,
	)
	return err
}

func (p *Pipeline) completeJob(job *models.SyncJob) {
	p.db.Exec(
		`UPDATE sync_jobs SET status = 'completed', updated_at = ? WHERE id = ?`,
		time.Now().UTC().Format(time.RFC3339), job.ID,
	)
}

func (p *Pipeline) failJob(job *models.SyncJob, errMsg string) {
	if len(errMsg) > 500 {
		errMsg = errMsg[:500]
	}
	newStatus := models.JobFailed
	if job.RetryCount+1 < maxJobRetries {
		newStatus = models.JobPending
	}
	p.db.Exec(`
		UPDATE sync_jobs
		SET status      = ?,
		    error_msg   = ?,
		    retry_count = retry_count + 1,
		    updated_at  = ?
		WHERE id = ?`,
		newStatus,
		strings.TrimSpace(errMsg),
		time.Now().UTC().Format(time.RFC3339),
		job.ID,
	)
}

func (p *Pipeline) setPipelineState(key, value string) {
	p.db.Exec(`
		INSERT INTO pipeline_state (key, value, updated_at)
		VALUES (?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET
		    value = excluded.value, updated_at = excluded.updated_at`,
		key, value, time.Now().UTC().Format(time.RFC3339),
	)
}


