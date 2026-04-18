package database

import (
	"database/sql"
	"fmt"
	"log"
)

// Each entry is idempotent (CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS).
// Append new entries; never edit existing ones.
var ddlStatements = []string{

	// ── Scheme registry ───────────────────────────────────────────────────────
	`CREATE TABLE IF NOT EXISTS schemes (
		code           TEXT PRIMARY KEY,
		name           TEXT NOT NULL,
		amc            TEXT NOT NULL,
		category       TEXT NOT NULL,
		last_synced_at TEXT,
		created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
	)`,

	// ── NAV time-series ───────────────────────────────────────────────────────
	`CREATE TABLE IF NOT EXISTS nav_records (
		scheme_code TEXT NOT NULL,
		date        TEXT NOT NULL,   -- ISO-8601 YYYY-MM-DD
		nav         REAL NOT NULL,
		PRIMARY KEY (scheme_code, date),
		FOREIGN KEY (scheme_code) REFERENCES schemes(code)
	)`,
	`CREATE INDEX IF NOT EXISTS idx_nav_scheme_date
		ON nav_records (scheme_code, date DESC)`,

	// ── Pre-computed analytics ────────────────────────────────────────────────
	`CREATE TABLE IF NOT EXISTS analytics (
		scheme_code     TEXT NOT NULL,
		window          TEXT NOT NULL,   -- 1Y|3Y|5Y|10Y
		data_start      TEXT,
		data_end        TEXT,
		total_days      INTEGER DEFAULT 0,
		nav_points      INTEGER DEFAULT 0,
		rolling_periods INTEGER DEFAULT 0,
		rolling_min     REAL    DEFAULT 0,
		rolling_max     REAL    DEFAULT 0,
		rolling_median  REAL    DEFAULT 0,
		rolling_p25     REAL    DEFAULT 0,
		rolling_p75     REAL    DEFAULT 0,
		max_drawdown    REAL    DEFAULT 0,
		cagr_min        REAL    DEFAULT 0,
		cagr_max        REAL    DEFAULT 0,
		cagr_median     REAL    DEFAULT 0,
		computed_at     TEXT,
		PRIMARY KEY (scheme_code, window),
		FOREIGN KEY (scheme_code) REFERENCES schemes(code)
	)`,

	// ── Pipeline job queue ────────────────────────────────────────────────────
	`CREATE TABLE IF NOT EXISTS sync_jobs (
		id          INTEGER PRIMARY KEY AUTOINCREMENT,
		scheme_code TEXT NOT NULL,
		job_type    TEXT NOT NULL,    -- backfill|incremental
		status      TEXT NOT NULL DEFAULT 'pending',
		error_msg   TEXT             DEFAULT '',
		retry_count INTEGER          DEFAULT 0,
		created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
		updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
	)`,
	`CREATE INDEX IF NOT EXISTS idx_sync_jobs_pending
		ON sync_jobs (status, created_at ASC)`,

	// ── Rate-limiter sliding-window persistence ───────────────────────────────
	`CREATE TABLE IF NOT EXISTS rate_limit_history (
		id           INTEGER PRIMARY KEY AUTOINCREMENT,
		requested_at TEXT NOT NULL   -- RFC3339Nano UTC
	)`,
	`CREATE INDEX IF NOT EXISTS idx_rl_history_time
		ON rate_limit_history (requested_at DESC)`,

	// ── General pipeline key-value state ─────────────────────────────────────
	`CREATE TABLE IF NOT EXISTS pipeline_state (
		key        TEXT PRIMARY KEY,
		value      TEXT NOT NULL,
		updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
	)`,
}

func applyMigrations(db *sql.DB) error {
	for i, stmt := range ddlStatements {
		if _, err := db.Exec(stmt); err != nil {
			snippet := stmt
			if len(snippet) > 80 {
				snippet = snippet[:80] + "…"
			}
			return fmt.Errorf("migration[%d]: %w  SQL: %s", i, err, snippet)
		}
	}
	log.Println("[db] schema up-to-date")
	return nil
}

