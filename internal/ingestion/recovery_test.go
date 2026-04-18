package ingestion

import (
	"database/sql"
	"testing"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Helper
// ─────────────────────────────────────────────────────────────────────────────

// insertJob inserts a sync_job row with the given status and returns its id.
func insertJob(t *testing.T, db interface {
	Exec(string, ...any) (sql.Result, error)
	QueryRow(string, ...any) *sql.Row
}, schemeCode, jobType, status string) int64 {
	t.Helper()
	res, err := db.Exec(`
		INSERT INTO sync_jobs (scheme_code, job_type, status)
		VALUES (?, ?, ?)`,
		schemeCode, jobType, status,
	)
	if err != nil {
		t.Fatalf("insertJob: %v", err)
	}
	id, _ := res.LastInsertId()
	return id
}

func jobStatus(t *testing.T, db interface {
	QueryRow(string, ...any) *sql.Row
}, id int64) string {
	t.Helper()
	var s string
	db.QueryRow(`SELECT status FROM sync_jobs WHERE id = ?`, id).Scan(&s)
	return s
}

// ─────────────────────────────────────────────────────────────────────────────
// RecoverStaleJobs
// ─────────────────────────────────────────────────────────────────────────────

func TestRecoverStaleJobs_ResetsRunningToPending(t *testing.T) {
	db := testDB(t)
	id := insertJob(t, db, "119598", "backfill", "running")

	stats, err := RecoverStaleJobs(db)
	if err != nil {
		t.Fatalf("RecoverStaleJobs: %v", err)
	}
	if stats.StaleJobsRecovered != 1 {
		t.Errorf("StaleJobsRecovered: want 1, got %d", stats.StaleJobsRecovered)
	}
	if got := jobStatus(t, db, id); got != "pending" {
		t.Errorf("job status after recovery: want pending, got %q", got)
	}
}

func TestRecoverStaleJobs_Idempotent(t *testing.T) {
	db := testDB(t)
	insertJob(t, db, "119598", "backfill", "running")

	// First call recovers 1 job.
	if stats, _ := RecoverStaleJobs(db); stats.StaleJobsRecovered != 1 {
		t.Fatalf("first recovery: want 1, got %d", stats.StaleJobsRecovered)
	}
	// Second call finds no running jobs – should be a no-op.
	if stats, err := RecoverStaleJobs(db); err != nil || stats.StaleJobsRecovered != 0 {
		t.Errorf("second recovery: want 0 recovered, got %d (err=%v)",
			stats.StaleJobsRecovered, err)
	}
}

func TestRecoverStaleJobs_DoesNotAffectOtherStatuses(t *testing.T) {
	db := testDB(t)

	pendingID := insertJob(t, db, "119598", "backfill", "pending")
	completedID := insertJob(t, db, "119598", "incremental", "completed")
	failedID := insertJob(t, db, "120505", "backfill", "failed")
	runningID := insertJob(t, db, "120505", "incremental", "running")

	if _, err := RecoverStaleJobs(db); err != nil {
		t.Fatalf("RecoverStaleJobs: %v", err)
	}

	checks := map[int64]string{
		pendingID:   "pending",
		completedID: "completed",
		failedID:    "failed",
		runningID:   "pending", // only this one changes
	}
	for id, want := range checks {
		if got := jobStatus(t, db, id); got != want {
			t.Errorf("job %d: want status %q, got %q", id, want, got)
		}
	}
}

func TestRecoverStaleJobs_IncrementsRetryCount(t *testing.T) {
	db := testDB(t)
	id := insertJob(t, db, "119598", "backfill", "running")

	RecoverStaleJobs(db)

	var retryCount int
	db.QueryRow(`SELECT retry_count FROM sync_jobs WHERE id = ?`, id).Scan(&retryCount)
	if retryCount != 1 {
		t.Errorf("retry_count after recovery: want 1, got %d", retryCount)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// PruneOldCompletedJobs
// ─────────────────────────────────────────────────────────────────────────────

func TestPruneOldCompletedJobs(t *testing.T) {
	db := testDB(t)

	// Insert one "old" completed job and one recent one.
	oldID := insertJob(t, db, "119598", "backfill", "completed")
	// Age it by rewriting updated_at to 10 days ago.
	db.Exec(`UPDATE sync_jobs SET updated_at = ? WHERE id = ?`,
		time.Now().Add(-10*24*time.Hour).UTC().Format(time.RFC3339), oldID)

	recentID := insertJob(t, db, "120505", "incremental", "completed")

	pruned, err := PruneOldCompletedJobs(db, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("PruneOldCompletedJobs: %v", err)
	}
	if pruned != 1 {
		t.Errorf("pruned count: want 1, got %d", pruned)
	}

	// Old job should be gone.
	if got := jobStatus(t, db, oldID); got != "" {
		t.Errorf("old job should be deleted, got status %q", got)
	}
	// Recent job should survive.
	if got := jobStatus(t, db, recentID); got != "completed" {
		t.Errorf("recent job should be completed, got %q", got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Checkpoint round-trip
// ─────────────────────────────────────────────────────────────────────────────

func TestCheckpointBackfill_RoundTrip(t *testing.T) {
	db := testDB(t)

	schemeCode := "119598"
	lastDate := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	inserted := 2513

	if err := CheckpointBackfill(db, schemeCode, lastDate, inserted); err != nil {
		t.Fatalf("CheckpointBackfill: %v", err)
	}

	gotDate, gotCount, err := GetBackfillCheckpoint(db, schemeCode)
	if err != nil {
		t.Fatalf("GetBackfillCheckpoint: %v", err)
	}
	if !gotDate.Equal(lastDate) {
		t.Errorf("date: want %v, got %v", lastDate, gotDate)
	}
	if gotCount != inserted {
		t.Errorf("count: want %d, got %d", inserted, gotCount)
	}
}

func TestGetBackfillCheckpoint_NoCheckpoint(t *testing.T) {
	db := testDB(t)

	// Non-existent scheme returns zero values, not an error.
	date, count, err := GetBackfillCheckpoint(db, "999999")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !date.IsZero() || count != 0 {
		t.Errorf("want zero values, got date=%v count=%d", date, count)
	}
}

// TestCheckpointBackfill_Upsert confirms that calling CheckpointBackfill twice
// overwrites the previous value rather than creating a duplicate.
func TestCheckpointBackfill_Upsert(t *testing.T) {
	db := testDB(t)

	schemeCode := "119598"
	date1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	date2 := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)

	CheckpointBackfill(db, schemeCode, date1, 100)
	CheckpointBackfill(db, schemeCode, date2, 500)

	gotDate, gotCount, err := GetBackfillCheckpoint(db, schemeCode)
	if err != nil {
		t.Fatalf("GetBackfillCheckpoint: %v", err)
	}
	if !gotDate.Equal(date2) {
		t.Errorf("date: want %v (latest), got %v", date2, gotDate)
	}
	if gotCount != 500 {
		t.Errorf("count: want 500, got %d", gotCount)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// ShouldSkipBackfill
// ─────────────────────────────────────────────────────────────────────────────

func TestShouldSkipBackfill_NoData(t *testing.T) {
	db := testDB(t)
	// Seed scheme so FK passes.
	db.Exec(`INSERT INTO schemes (code, name, amc, category) VALUES ('A', 'Test', 'AMC', 'Mid Cap')`)

	skip, err := ShouldSkipBackfill(db, "A", 100)
	if err != nil {
		t.Fatalf("ShouldSkipBackfill: %v", err)
	}
	if skip {
		t.Error("expected false (no data exists), got true")
	}
}

func TestShouldSkipBackfill_FreshRecentData(t *testing.T) {
	db := testDB(t)
	db.Exec(`INSERT INTO schemes (code, name, amc, category) VALUES ('A', 'Test', 'AMC', 'Mid Cap')`)

	// Insert 200 records; latest is from yesterday.
	yesterday := time.Now().AddDate(0, 0, -1).Format("2006-01-02")
	for i := 0; i < 200; i++ {
		d := time.Now().AddDate(0, 0, -(200 - i)).Format("2006-01-02")
		db.Exec(`INSERT INTO nav_records (scheme_code, date, nav) VALUES ('A', ?, 100)`, d)
	}
	db.Exec(`INSERT OR REPLACE INTO nav_records (scheme_code, date, nav) VALUES ('A', ?, 100)`, yesterday)

	skip, err := ShouldSkipBackfill(db, "A", 100)
	if err != nil {
		t.Fatalf("ShouldSkipBackfill: %v", err)
	}
	if !skip {
		t.Error("expected true (fresh data with enough records), got false")
	}
}

func TestShouldSkipBackfill_StaleData(t *testing.T) {
	db := testDB(t)
	db.Exec(`INSERT INTO schemes (code, name, amc, category) VALUES ('A', 'Test', 'AMC', 'Mid Cap')`)

	// 200 records but latest is 30 days ago.
	for i := 0; i < 200; i++ {
		d := time.Now().AddDate(0, 0, -(230 - i)).Format("2006-01-02")
		db.Exec(`INSERT OR IGNORE INTO nav_records (scheme_code, date, nav) VALUES ('A', ?, 100)`, d)
	}

	skip, err := ShouldSkipBackfill(db, "A", 100)
	if err != nil {
		t.Fatalf("ShouldSkipBackfill: %v", err)
	}
	if skip {
		t.Error("expected false (data is stale), got true")
	}
}

func TestShouldSkipBackfill_InsufficientRecords(t *testing.T) {
	db := testDB(t)
	db.Exec(`INSERT INTO schemes (code, name, amc, category) VALUES ('A', 'Test', 'AMC', 'Mid Cap')`)

	// Only 5 records (recent but too few).
	for i := 0; i < 5; i++ {
		d := time.Now().AddDate(0, 0, -i).Format("2006-01-02")
		db.Exec(`INSERT OR IGNORE INTO nav_records (scheme_code, date, nav) VALUES ('A', ?, 100)`, d)
	}

	skip, err := ShouldSkipBackfill(db, "A", 100)
	if err != nil {
		t.Fatalf("ShouldSkipBackfill: %v", err)
	}
	if skip {
		t.Error("expected false (insufficient records), got true")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// ResetFailedJobs
// ─────────────────────────────────────────────────────────────────────────────

func TestResetFailedJobs_AllFailed(t *testing.T) {
	db := testDB(t)
	id1 := insertJob(t, db, "119598", "backfill", "failed")
	id2 := insertJob(t, db, "120505", "backfill", "failed")

	n, err := ResetFailedJobs(db, "")
	if err != nil {
		t.Fatalf("ResetFailedJobs: %v", err)
	}
	if n != 2 {
		t.Errorf("want 2 reset, got %d", n)
	}
	for _, id := range []int64{id1, id2} {
		if got := jobStatus(t, db, id); got != "pending" {
			t.Errorf("job %d: want pending, got %q", id, got)
		}
	}
}

func TestResetFailedJobs_SpecificScheme(t *testing.T) {
	db := testDB(t)
	target := insertJob(t, db, "119598", "backfill", "failed")
	other := insertJob(t, db, "120505", "backfill", "failed")

	n, err := ResetFailedJobs(db, "119598")
	if err != nil {
		t.Fatalf("ResetFailedJobs: %v", err)
	}
	if n != 1 {
		t.Errorf("want 1 reset, got %d", n)
	}
	if got := jobStatus(t, db, target); got != "pending" {
		t.Errorf("target: want pending, got %q", got)
	}
	if got := jobStatus(t, db, other); got != "failed" {
		t.Errorf("other: want failed (unchanged), got %q", got)
	}
}

