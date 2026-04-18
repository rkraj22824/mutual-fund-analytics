package ingestion

import (
	"context"
	"log"
	"sync"
	"time"

	"mf-analytics/internal/config"
	"mf-analytics/internal/database"
	"mf-analytics/internal/models"
)

// RateLimiter enforces three concurrent sliding-window limits:
//
//	2  requests / second
//	50 requests / minute
//	300 requests / hour
//
// All three windows are checked on every Wait() call and the call blocks
// until every window simultaneously allows the next request.
//
// State is persisted to SQLite so quota consumption survives process restarts.
//
// nowFn and afterFn are dependency-injected so tests can drive time forward
// instantaneously without any real sleeps.
type RateLimiter struct {
	mu           sync.Mutex
	db           *database.DB      // nil = no persistence (tests)
	requests     []time.Time       // sliding window – all requests within last hour
	perSecond    int
	perMinute    int
	perHour      int
	blockedUntil *time.Time        // non-nil when a 429 penalty block is active

	// Injectable for deterministic unit tests.
	// Production: nowFn=time.Now, afterFn=time.After
	nowFn   func() time.Time
	afterFn func(time.Duration) <-chan time.Time
}

// NewRateLimiter creates a production RateLimiter with a real clock and
// restores persisted sliding-window state from the database.
func NewRateLimiter(cfg *config.Config, db *database.DB) *RateLimiter {
	rl := &RateLimiter{
		db:        db,
		perSecond: cfg.RatePerSecond,
		perMinute: cfg.RatePerMinute,
		perHour:   cfg.RatePerHour,
		nowFn:     time.Now,
		afterFn:   time.After,
	}
	if err := rl.restoreState(); err != nil {
		log.Printf("[ratelimiter] could not restore state (starting fresh): %v", err)
	}
	return rl
}

// restoreState loads request timestamps from the past hour so that quota
// consumed before a restart is correctly accounted for.
func (rl *RateLimiter) restoreState() error {
	if rl.db == nil {
		return nil
	}
	cutoff := rl.nowFn().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	rows, err := rl.db.Query(
		`SELECT requested_at FROM rate_limit_history
		 WHERE requested_at > ? ORDER BY requested_at ASC`,
		cutoff,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.requests = rl.requests[:0]

	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			continue
		}
		t, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			continue
		}
		rl.requests = append(rl.requests, t)
	}
	log.Printf("[ratelimiter] restored %d requests from last hour", len(rl.requests))
	return rows.Err()
}

// persistRequest writes a timestamp to the DB asynchronously so that
// Wait() is not delayed by the write. Entries older than 2 h are pruned.
func (rl *RateLimiter) persistRequest(ts time.Time) {
	if rl.db == nil {
		return
	}
	go func() {
		_, err := rl.db.Exec(
			`INSERT INTO rate_limit_history (requested_at) VALUES (?)`,
			ts.UTC().Format(time.RFC3339Nano),
		)
		if err != nil {
			log.Printf("[ratelimiter] persist: %v", err)
		}
		cutoff := rl.nowFn().Add(-2 * time.Hour).UTC().Format(time.RFC3339Nano)
		_, _ = rl.db.Exec(
			`DELETE FROM rate_limit_history WHERE requested_at < ?`, cutoff,
		)
	}()
}

// SetBlocked signals that the remote has imposed a quota-exhaustion block
// that expires at `until`. Wait() will sleep until after this time before
// reapplying its normal per-window checks.
func (rl *RateLimiter) SetBlocked(until time.Time) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.blockedUntil = &until
	log.Printf("[ratelimiter] BLOCKED until %s", until.Format(time.RFC3339))
}

// Wait blocks until ALL three rate-limit windows simultaneously allow the
// next request, then atomically records it. Safe for concurrent callers.
func (rl *RateLimiter) Wait(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		rl.mu.Lock()
		now := rl.nowFn()

		// ── Global 429 penalty block ──────────────────────────────────────────
		if rl.blockedUntil != nil && now.Before(*rl.blockedUntil) {
			wait := rl.blockedUntil.Sub(now) + 50*time.Millisecond
			rl.mu.Unlock()
			log.Printf("[ratelimiter] globally blocked, sleeping %s", wait.Round(time.Second))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-rl.afterFn(wait):
			}
			continue
		}
		if rl.blockedUntil != nil {
			rl.blockedUntil = nil
		}

		// ── Evict timestamps older than 1 hour ────────────────────────────────
		hourAgo := now.Add(-time.Hour)
		n := 0
		for _, t := range rl.requests {
			if t.After(hourAgo) {
				rl.requests[n] = t
				n++
			}
		}
		rl.requests = rl.requests[:n]

		// ── Count requests in each window; track oldest per-window entry ──────
		secAgo := now.Add(-time.Second)
		minAgo := now.Add(-time.Minute)

		var c1s, c1m, c1h int
		oldestSec := now
		oldestMin := now
		oldestHour := now

		for _, t := range rl.requests {
			// hourly bucket
			c1h++
			if t.Before(oldestHour) {
				oldestHour = t
			}
			// per-minute bucket
			if t.After(minAgo) {
				c1m++
				if t.Before(oldestMin) {
					oldestMin = t
				}
			}
			// per-second bucket
			if t.After(secAgo) {
				c1s++
				if t.Before(oldestSec) {
					oldestSec = t
				}
			}
		}

		// ── Compute the minimum required wait (pick the most restrictive) ─────
		var waitDur time.Duration

		enforce := func(count, limit int, oldest time.Time, window time.Duration) {
			if count >= limit {
				// The oldest request in this window will expire at oldest+window.
				// We must wait until that moment (plus 1 ms guard) has passed.
				if d := oldest.Add(window).Sub(now) + time.Millisecond; d > waitDur {
					waitDur = d
				}
			}
		}
		enforce(c1s, rl.perSecond, oldestSec, time.Second)
		enforce(c1m, rl.perMinute, oldestMin, time.Minute)
		enforce(c1h, rl.perHour, oldestHour, time.Hour)

		if waitDur > 0 {
			rl.mu.Unlock()
			log.Printf("[ratelimiter] throttling %s  (1s:%d/%d  1m:%d/%d  1h:%d/%d)",
				waitDur.Round(time.Millisecond),
				c1s, rl.perSecond, c1m, rl.perMinute, c1h, rl.perHour,
			)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-rl.afterFn(waitDur):
			}
			continue
		}

		// ── All limits satisfied – record and return ──────────────────────────
		rl.requests = append(rl.requests, now)
		rl.mu.Unlock()

		rl.persistRequest(now)
		return nil
	}
}

// Status returns a non-blocking snapshot of current window consumption.
func (rl *RateLimiter) Status() models.RateLimitStatus {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := rl.nowFn()
	secAgo := now.Add(-time.Second)
	minAgo := now.Add(-time.Minute)
	hourAgo := now.Add(-time.Hour)

	var c1s, c1m, c1h int
	for _, t := range rl.requests {
		if t.After(hourAgo) {
			c1h++
		}
		if t.After(minAgo) {
			c1m++
		}
		if t.After(secAgo) {
			c1s++
		}
	}

	pos := func(a, b int) int {
		if a > b {
			return a
		}
		return b
	}
	return models.RateLimitStatus{
		UsedLastSecond: c1s,
		UsedLastMinute: c1m,
		UsedLastHour:   c1h,
		FreePerSecond:  pos(0, rl.perSecond-c1s),
		FreePerMinute:  pos(0, rl.perMinute-c1m),
		FreePerHour:    pos(0, rl.perHour-c1h),
	}
}

