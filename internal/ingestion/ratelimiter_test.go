package ingestion

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mf-analytics/internal/config"
	"mf-analytics/internal/database"
)

// ─────────────────────────────────────────────────────────────────────────────
// Test helpers
// ─────────────────────────────────────────────────────────────────────────────

// fakeTime is a thread-safe, controllable clock.
// After() advances the clock instantly so Wait() never does a real sleep.
type fakeTime struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeTime(t time.Time) *fakeTime { return &fakeTime{now: t} }

func (f *fakeTime) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeTime) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// After advances the clock by d and immediately returns a fired channel.
// This replaces time.After inside Wait() so tests run without real sleeps.
func (f *fakeTime) After(d time.Duration) <-chan time.Time {
	f.mu.Lock()
	f.now = f.now.Add(d)
	t := f.now
	f.mu.Unlock()
	ch := make(chan time.Time, 1)
	ch <- t
	return ch
}

// makeRL builds a RateLimiter with a fake clock and no DB (no persistence).
func makeRL(perSec, perMin, perHour int, ft *fakeTime) *RateLimiter {
	return &RateLimiter{
		db:        nil, // persistence disabled; guarded in persistRequest / restoreState
		perSecond: perSec,
		perMinute: perMin,
		perHour:   perHour,
		nowFn:     ft.Now,
		afterFn:   ft.After,
	}
}

// testDB creates a fresh in-memory SQLite DB and registers cleanup.
func testDB(t *testing.T) *database.DB {
	t.Helper()
	db, err := database.New(":memory:")
	if err != nil {
		t.Fatalf("testDB: %v", err)
	}
	if err := db.RunMigrations(); err != nil {
		t.Fatalf("testDB migrations: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests – single-threaded, fake clock
// ─────────────────────────────────────────────────────────────────────────────

// TestPerSecondLimit verifies that filling the 1-second window forces Wait()
// to sleep until the oldest in-window request expires.
func TestPerSecondLimit(t *testing.T) {
	t0 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	ft := newFakeTime(t0)
	rl := makeRL(2, 200, 2000, ft) // perSec=2; min/hour limits generous

	// Pre-fill: two requests 500 ms and 200 ms ago → window is full.
	rl.requests = []time.Time{
		t0.Add(-500 * time.Millisecond),
		t0.Add(-200 * time.Millisecond),
	}

	if err := rl.Wait(context.Background()); err != nil {
		t.Fatalf("Wait() error: %v", err)
	}

	// After(d) advanced the fake clock by the throttle duration.
	// Oldest entry was at t0-500ms → window expires at t0+500ms.
	// So fake clock must now be ≥ t0+500ms.
	if ft.Now().Before(t0.Add(500 * time.Millisecond)) {
		t.Errorf("expected fake clock ≥ t0+500ms after Wait(), got %v ahead of t0",
			ft.Now().Sub(t0))
	}

	// Request was recorded.
	if got := len(rl.requests); got != 3 {
		t.Errorf("requests slice: want 3 entries, got %d", got)
	}
}

// TestPerMinuteLimit verifies that filling the 1-minute window forces a wait
// even when the per-second and per-hour windows are well below their limits.
func TestPerMinuteLimit(t *testing.T) {
	t0 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	ft := newFakeTime(t0)
	rl := makeRL(100, 3, 1000, ft) // perMin=3 (tight); sec/hour generous

	// Pre-fill: three requests evenly spread over the last 50 seconds.
	// Per-second count = 0 (all older than 1 s). Per-minute count = 3 = limit.
	rl.requests = []time.Time{
		t0.Add(-50 * time.Second),
		t0.Add(-30 * time.Second),
		t0.Add(-10 * time.Second),
	}

	if err := rl.Wait(context.Background()); err != nil {
		t.Fatalf("Wait() error: %v", err)
	}

	// Oldest in-minute was at t0-50s → expires at t0+10s.
	if ft.Now().Before(t0.Add(10 * time.Second)) {
		t.Errorf("expected clock ≥ t0+10s, got %v ahead of t0", ft.Now().Sub(t0))
	}
}

// TestPerHourLimit verifies that filling the 1-hour window forces a wait
// even when both shorter windows are empty.
func TestPerHourLimit(t *testing.T) {
	t0 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	ft := newFakeTime(t0)
	rl := makeRL(100, 200, 4, ft) // perHour=4 (tight); sec/min generous

	// Four requests spread over the last 55 minutes – hourly window is full,
	// but they are all > 1 minute old so sec/min windows are empty.
	rl.requests = []time.Time{
		t0.Add(-55 * time.Minute),
		t0.Add(-40 * time.Minute),
		t0.Add(-25 * time.Minute),
		t0.Add(-10 * time.Minute),
	}

	if err := rl.Wait(context.Background()); err != nil {
		t.Fatalf("Wait() error: %v", err)
	}

	// Oldest in-hour was at t0-55min → expires at t0+5min.
	if ft.Now().Before(t0.Add(5 * time.Minute)) {
		t.Errorf("expected clock ≥ t0+5min, got %v ahead of t0", ft.Now().Sub(t0))
	}
}

// TestMostRestrictiveLimitWins verifies that when multiple windows are
// simultaneously full, Wait() waits for the longest required duration.
func TestMostRestrictiveLimitWins(t *testing.T) {
	t0 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	ft := newFakeTime(t0)
	// perSec=2 (full), perMin=3 (full with wider gap), perHour=100 (fine)
	rl := makeRL(2, 3, 100, ft)

	// Per-second window full: oldest at t0-800ms (expires t0+200ms).
	// Per-minute window full: oldest at t0-58s   (expires t0+2s).
	// Per-minute wait is longer → must win.
	rl.requests = []time.Time{
		t0.Add(-58 * time.Second), // minute-oldest
		t0.Add(-30 * time.Second),
		t0.Add(-800 * time.Millisecond), // second-oldest (also the 3rd minute entry)
		t0.Add(-400 * time.Millisecond), // 4th entry – second full
	}
	// c1s = 2 (last 2), c1m = 4 → wait both; c1m limit=3 BUT we have 4 which >limit,
	// actually let me recalculate.
	// With perMin=3: c1m = entries after t0-60s = all 4.  4 >= 3 → triggered.
	// oldest_min = t0-58s, expiry = t0+2s, waitDur=2s+1ms
	// With perSec=2: c1s = entries after t0-1s = last 2. 2 >= 2 → triggered.
	// oldest_sec = t0-800ms, expiry = t0+200ms+1ms, waitDur=201ms
	// Most restrictive is perMin: 2s+1ms
	// After fakeTime.After(2s+1ms) fires, clock is t0+2s+1ms.
	// Re-check: oldest_min was t0-58s, now it's before (t0+2s+1ms)-60s = t0-57.999s
	// so c1m should drop below 3.

	if err := rl.Wait(context.Background()); err != nil {
		t.Fatalf("Wait() error: %v", err)
	}

	// Clock must have advanced by at least 2 seconds (the minute-limit wait).
	if ft.Now().Before(t0.Add(2 * time.Second)) {
		t.Errorf("expected clock ≥ t0+2s, got %v ahead of t0", ft.Now().Sub(t0))
	}
}

// TestNoDelayWhenBelowAllLimits confirms that Wait() returns immediately
// (no afterFn call) when all windows are below their limits.
func TestNoDelayWhenBelowAllLimits(t *testing.T) {
	t0 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	var afterCalled int64
	countingAfter := func(d time.Duration) <-chan time.Time {
		atomic.AddInt64(&afterCalled, 1)
		ch := make(chan time.Time, 1)
		ch <- t0.Add(d)
		return ch
	}

	rl := &RateLimiter{
		perSecond: 10,
		perMinute: 100,
		perHour:   500,
		nowFn:     func() time.Time { return t0 },
		afterFn:   countingAfter,
	}
	// One old request – well below all limits.
	rl.requests = []time.Time{t0.Add(-5 * time.Second)}

	if err := rl.Wait(context.Background()); err != nil {
		t.Fatalf("Wait() error: %v", err)
	}
	if atomic.LoadInt64(&afterCalled) != 0 {
		t.Errorf("afterFn was called %d time(s); expected 0 (no throttling needed)",
			atomic.LoadInt64(&afterCalled))
	}
}

// TestSetBlocked verifies that a global block imposed by SetBlocked causes
// Wait() to sleep until the block expires.
func TestSetBlocked(t *testing.T) {
	t0 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	ft := newFakeTime(t0)
	rl := makeRL(10, 100, 500, ft)

	blockExpiry := t0.Add(5 * time.Second)
	rl.SetBlocked(blockExpiry)

	if err := rl.Wait(context.Background()); err != nil {
		t.Fatalf("Wait() error: %v", err)
	}

	// Clock must have advanced past the block expiry.
	if ft.Now().Before(blockExpiry) {
		t.Errorf("expected clock ≥ blockExpiry (%v), got %v", blockExpiry, ft.Now())
	}
	// blockedUntil should be cleared after expiry.
	rl.mu.Lock()
	blocked := rl.blockedUntil
	rl.mu.Unlock()
	if blocked != nil {
		t.Error("blockedUntil should be nil after block has expired")
	}
}

// TestContextCancellation confirms that Wait() returns ctx.Err() when the
// context is cancelled while waiting for a rate-limit window to open.
func TestContextCancellation(t *testing.T) {
	t0 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	// afterFn that never fires – simulates a long wait.
	neverFire := func(d time.Duration) <-chan time.Time {
		return make(chan time.Time) // unbuffered, never written
	}

	rl := &RateLimiter{
		perSecond: 2,
		perMinute: 200,
		perHour:   2000,
		nowFn:     func() time.Time { return t0 },
		afterFn:   neverFire,
	}
	// Fill the per-second window so Wait() tries to sleep.
	rl.requests = []time.Time{
		t0.Add(-500 * time.Millisecond),
		t0.Add(-200 * time.Millisecond),
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- rl.Wait(ctx) }()

	// Cancel context after a short real delay.
	time.AfterFunc(20*time.Millisecond, cancel)

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Errorf("Wait() returned %v; want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait() did not return after context cancellation (deadlock?)")
	}
}

// TestStatus verifies the Status snapshot reflects the current window usage.
func TestStatus(t *testing.T) {
	t0 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	ft := newFakeTime(t0)
	rl := makeRL(2, 50, 300, ft)

	// 3 requests: 1 within last second, 2 within last minute, 3 within last hour.
	rl.requests = []time.Time{
		t0.Add(-45 * time.Minute), // only in hour
		t0.Add(-30 * time.Second), // in hour + minute
		t0.Add(-500 * time.Millisecond), // in hour + minute + second
	}

	s := rl.Status()

	if s.UsedLastSecond != 1 {
		t.Errorf("UsedLastSecond: want 1, got %d", s.UsedLastSecond)
	}
	if s.UsedLastMinute != 2 {
		t.Errorf("UsedLastMinute: want 2, got %d", s.UsedLastMinute)
	}
	if s.UsedLastHour != 3 {
		t.Errorf("UsedLastHour: want 3, got %d", s.UsedLastHour)
	}
	if s.FreePerSecond != 1 {
		t.Errorf("FreePerSecond: want 1, got %d", s.FreePerSecond)
	}
	if s.FreePerMinute != 48 {
		t.Errorf("FreePerMinute: want 48, got %d", s.FreePerMinute)
	}
	if s.FreePerHour != 297 {
		t.Errorf("FreePerHour: want 297, got %d", s.FreePerHour)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Concurrent-access tests – real clock, race-detector safe
// ─────────────────────────────────────────────────────────────────────────────

// TestConcurrentAccess_NoRaces is designed to be run with `go test -race`.
// It spins up many goroutines that call Wait() simultaneously and verifies
// there are no data races and no deadlocks.
func TestConcurrentAccess_NoRaces(t *testing.T) {
	// Very generous limits so calls complete without throttling.
	rl := &RateLimiter{
		perSecond: 50,
		perMinute: 500,
		perHour:   5000,
		nowFn:     time.Now,
		afterFn:   time.After,
	}

	const goroutines = 20
	var wg sync.WaitGroup
	var succeeded int64

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := rl.Wait(ctx); err == nil {
				atomic.AddInt64(&succeeded, 1)
			}
		}()
	}

	deadlock := make(chan struct{})
	go func() {
		wg.Wait()
		close(deadlock)
	}()

	select {
	case <-deadlock:
		// all goroutines finished
	case <-time.After(10 * time.Second):
		t.Fatal("goroutines deadlocked")
	}

	if got := atomic.LoadInt64(&succeeded); got != goroutines {
		t.Errorf("succeeded: want %d, got %d", goroutines, got)
	}
}

// TestConcurrentAccess_LimitsRespected uses real time to verify that concurrent
// callers never collectively exceed the per-second limit.
// This test intentionally takes ~3 seconds; skip with -short.
func TestConcurrentAccess_LimitsRespected(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real-time concurrent limit test in -short mode")
	}

	const perSec = 3
	rl := &RateLimiter{
		perSecond: perSec,
		perMinute: 60,
		perHour:   300,
		nowFn:     time.Now,
		afterFn:   time.After,
	}

	const total = 9 // 3 batches → takes ~3 s
	var (
		mu          sync.Mutex
		completedAt []time.Time
		wg          sync.WaitGroup
	)

	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if err := rl.Wait(ctx); err != nil {
				t.Errorf("Wait(): %v", err)
				return
			}
			mu.Lock()
			completedAt = append(completedAt, time.Now())
			mu.Unlock()
		}()
	}
	wg.Wait()

	if len(completedAt) != total {
		t.Fatalf("expected %d completions, got %d", total, len(completedAt))
	}

	// For every recorded timestamp T, count how many completed in [T, T+1s).
	// That count must never exceed perSec.
	for i, ts := range completedAt {
		var count int
		for j, other := range completedAt {
			if i == j {
				continue
			}
			diff := other.Sub(ts)
			if diff >= 0 && diff < time.Second {
				count++
			}
		}
		// count is "others" in the window; adding the reference timestamp itself
		// gives (count + 1) total requests in that 1-second view.
		if count+1 > perSec {
			t.Errorf("per-second limit violated: %d requests within 1s of timestamp %d",
				count+1, i)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// State-persistence tests – real DB
// ─────────────────────────────────────────────────────────────────────────────

// TestStatePersistence_RestoresFromDB verifies that a new RateLimiter instance
// reading from the same DB sees the usage that was recorded by a previous one.
func TestStatePersistence_RestoresFromDB(t *testing.T) {
	db := testDB(t)
	now := time.Now().UTC()

	// Directly insert 5 request timestamps (5–45 s ago) into the DB.
	// All are within the last minute but none within the last second.
	for i := 0; i < 5; i++ {
		ts := now.Add(-time.Duration(5+i*10) * time.Second)
		_, err := db.Exec(
			`INSERT INTO rate_limit_history (requested_at) VALUES (?)`,
			ts.Format(time.RFC3339Nano),
		)
		if err != nil {
			t.Fatalf("insert history: %v", err)
		}
	}

	cfg := &config.Config{RatePerSecond: 2, RatePerMinute: 50, RatePerHour: 300}
	rl := NewRateLimiter(cfg, db)

	s := rl.Status()

	// All 5 entries are within the last hour AND the last minute.
	if s.UsedLastHour != 5 {
		t.Errorf("UsedLastHour: want 5, got %d", s.UsedLastHour)
	}
	if s.UsedLastMinute != 5 {
		t.Errorf("UsedLastMinute: want 5, got %d", s.UsedLastMinute)
	}
	// The earliest entry is 45 s ago → outside the 1-second window.
	if s.UsedLastSecond != 0 {
		t.Errorf("UsedLastSecond: want 0, got %d (restored entries should not be in last second)",
			s.UsedLastSecond)
	}
	if s.FreePerSecond != 2 {
		t.Errorf("FreePerSecond: want 2, got %d", s.FreePerSecond)
	}
}

// TestStatePersistence_StaleEntriesIgnored verifies that entries older than
// 1 hour are NOT loaded into the in-memory window on restore.
func TestStatePersistence_StaleEntriesIgnored(t *testing.T) {
	db := testDB(t)
	now := time.Now().UTC()

	// Insert 3 very old entries (2-3 hours ago) – outside any window.
	for i := 2; i <= 4; i++ {
		ts := now.Add(-time.Duration(i) * time.Hour)
		db.Exec(`INSERT INTO rate_limit_history (requested_at) VALUES (?)`,
			ts.Format(time.RFC3339Nano))
	}

	cfg := &config.Config{RatePerSecond: 2, RatePerMinute: 50, RatePerHour: 300}
	rl := NewRateLimiter(cfg, db)

	s := rl.Status()
	if s.UsedLastHour != 0 {
		t.Errorf("UsedLastHour: want 0 (stale entries should be ignored), got %d",
			s.UsedLastHour)
	}
}

// TestStatePersistence_RequestsPersisted verifies that actual calls to Wait()
// write timestamps to the DB that a subsequent RateLimiter can see.
func TestStatePersistence_RequestsPersisted(t *testing.T) {
	db := testDB(t)
	cfg := &config.Config{RatePerSecond: 10, RatePerMinute: 100, RatePerHour: 500}

	rl1 := NewRateLimiter(cfg, db)

	// Make 3 requests (well within limits).
	for i := 0; i < 3; i++ {
		if err := rl1.Wait(context.Background()); err != nil {
			t.Fatalf("Wait() #%d: %v", i, err)
		}
	}

	// Wait briefly for async persistence goroutines to complete.
	time.Sleep(100 * time.Millisecond)

	// A brand-new RateLimiter from the same DB should see those 3 entries.
	rl2 := NewRateLimiter(cfg, db)
	s := rl2.Status()
	if s.UsedLastHour < 3 {
		t.Errorf("restored UsedLastHour: want ≥3, got %d", s.UsedLastHour)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmarks
// ─────────────────────────────────────────────────────────────────────────────

// BenchmarkWait_NoThrottling measures the overhead of Wait() when all
// windows are empty (the common hot path during low-traffic periods).
func BenchmarkWait_NoThrottling(b *testing.B) {
	rl := &RateLimiter{
		perSecond: 1_000_000, // effectively unlimited
		perMinute: 1_000_000,
		perHour:   1_000_000,
		nowFn:     time.Now,
		afterFn:   time.After,
	}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := rl.Wait(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWait_NearLimit measures the overhead when windows are close to
// (but not at) their limits – the steady-state production case.
func BenchmarkWait_NearLimit(b *testing.B) {
	t0 := time.Now()
	rl := &RateLimiter{
		perSecond: 2,
		perMinute: 50,
		perHour:   300,
		nowFn:     time.Now,
		afterFn:   time.After,
	}
	// Pre-fill with one recent request so counts > 0 but below limits.
	rl.requests = []time.Time{t0.Add(-600 * time.Millisecond)}

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Keep clearing the slice so we never actually hit the limit.
		rl.mu.Lock()
		rl.requests = rl.requests[:1]
		rl.mu.Unlock()
		if err := rl.Wait(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

