package ingestion

import (
	"fmt"
	"log"
	"time"

	"mf-analytics/internal/database"
)

// RecoveryStats summarises the outcome of a crash-recovery pass.
type RecoveryStats struct {
	// StaleJobsRecovered is the number of jobs that were stuck in "running"
	// and have been reset to "pending" for re-execution.
	StaleJobsRecovered int
}

// RecoverStaleJobs resets any sync_jobs that are stuck in the "running" state
// back to "pending". A job becomes stuck when the process crashes (SIGKILL,
// OOM, panic) while that job is being executed.
//
// Idempotent: if no running jobs exist the call is a no-op.
// The retry_count is incremented so that persistently failing jobs eventually
// hit maxJobRetries and are quarantined as "failed".
func RecoverStaleJobs(db *database.DB) (RecoveryStats, error) {
	result, err := db.Exec(`
		UPDATE sync_jobs
		SET status      = 'pending',
		    error_msg   = 'auto-recovered: process restart',
		    retry_count = retry_count + 1,
		    updated_at  = ?
		WHERE status = 'running'`,
		time.Now().UTC().Format(time.RFC3339),
	)
	if err != nil {
		return RecoveryStats{}, fmt.Errorf("recover stale jobs: %w", err)
	}

	n, _ := result.RowsAffected()
	if n > 0 {
		log.Printf("[recovery] reset %d stale running job(s) → pending", n)
	}
	return RecoveryStats{StaleJobsRecovered: int(n)}, nil
}

// PruneOldCompletedJobs deletes completed jobs older than `age` to prevent
// the sync_jobs table from growing unbounded.
// Typical production call: PruneOldCompletedJobs(db, 7*24*time.Hour).
func PruneOldCompletedJobs(db *database.DB, age time.Duration) (int, error) {
	cutoff := time.Now().Add(-age).UTC().Format(time.RFC3339)
	result, err := db.Exec(`
		DELETE FROM sync_jobs
		WHERE status = 'completed' AND updated_at < ?`,
		cutoff,
	)
	if err != nil {
		return 0, fmt.Errorf("prune completed jobs: %w", err)
	}
	n, _ := result.RowsAffected()
	if n > 0 {
		log.Printf("[recovery] pruned %d old completed job(s)", n)
	}
	return int(n), nil
}

// ResetFailedJobs resets permanently-failed jobs back to "pending" so they
// can be retried. Pass an empty schemeCode to reset all failed jobs.
func ResetFailedJobs(db *database.DB, schemeCode string) (int, error) {
	var (
		query = `UPDATE sync_jobs
		         SET status = 'pending', retry_count = 0, updated_at = ?
		         WHERE status = 'failed'`
		args = []any{time.Now().UTC().Format(time.RFC3339)}
	)
	if schemeCode != "" {
		query += " AND scheme_code = ?"
		args = append(args, schemeCode)
	}
	result, err := db.Exec(query, args...)
	if err != nil {
		return 0, fmt.Errorf("reset failed jobs: %w", err)
	}
	n, _ := result.RowsAffected()
	if n > 0 {
		log.Printf("[recovery] reset %d failed job(s) to pending", n)
	}
	return int(n), nil
}

// CheckpointBackfill persists the last-inserted NAV date for a scheme so
// that a resumed backfill can skip already-fetched data without any wasted
// API calls. The checkpoint is stored in the pipeline_state key-value table.
func CheckpointBackfill(db *database.DB, schemeCode string, lastDate time.Time, insertedCount int) error {
	value := fmt.Sprintf("%s|%d", lastDate.Format("2006-01-02"), insertedCount)
	_, err := db.Exec(`
		INSERT INTO pipeline_state (key, value, updated_at)
		VALUES (?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET
		    value      = excluded.value,
		    updated_at = excluded.updated_at`,
		checkpointKey(schemeCode), value,
		time.Now().UTC().Format(time.RFC3339),
	)
	return err
}

// GetBackfillCheckpoint returns the last-known backfill progress for a scheme.
// Returns (zero time, 0, nil) when no checkpoint exists yet.
func GetBackfillCheckpoint(db *database.DB, schemeCode string) (lastDate time.Time, insertedCount int, err error) {
	var value string
	err = db.QueryRow(
		`SELECT value FROM pipeline_state WHERE key = ?`,
		checkpointKey(schemeCode),
	).Scan(&value)
	if err != nil {
		// No row is normal for a first run; surface other errors.
		return time.Time{}, 0, nil
	}
	var dateStr string
	if _, scanErr := fmt.Sscanf(value, "%10s|%d", &dateStr, &insertedCount); scanErr != nil {
		return time.Time{}, 0, fmt.Errorf("malformed checkpoint %q: %w", value, scanErr)
	}
	lastDate, err = time.Parse("2006-01-02", dateStr)
	return
}

// ShouldSkipBackfill returns true when the scheme already has enough recent
// data and a full re-backfill would be wasteful.
//
// The heuristic is:
//  1. At least `minRecords` NAV records exist.
//  2. The most recent record is within the last 7 calendar days.
//
// If either condition fails the caller should (re-)run the backfill.
func ShouldSkipBackfill(db *database.DB, schemeCode string, minRecords int) (bool, error) {
	var count int
	var latestRaw string

	err := db.QueryRow(`
		SELECT COUNT(*), COALESCE(MAX(date), '')
		FROM nav_records
		WHERE scheme_code = ?`,
		schemeCode,
	).Scan(&count, &latestRaw)
	if err != nil {
		return false, fmt.Errorf("check backfill skip %s: %w", schemeCode, err)
	}

	if count < minRecords || latestRaw == "" {
		return false, nil
	}

	latest, err := time.Parse("2006-01-02", latestRaw)
	if err != nil {
		return false, nil
	}

	// Data is fresh if the latest record is within the last 7 days.
	fresh := time.Since(latest) < 7*24*time.Hour
	return fresh, nil
}

// clearCheckpoint removes the backfill checkpoint for a scheme once the
// backfill has completed successfully.
func clearCheckpoint(db *database.DB, schemeCode string) {
	db.Exec(`DELETE FROM pipeline_state WHERE key = ?`, checkpointKey(schemeCode))
}

func checkpointKey(schemeCode string) string {
	return "backfill_checkpoint:" + schemeCode
}

