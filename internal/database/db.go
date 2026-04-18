package database

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite" // pure-Go SQLite driver
)

// DB wraps *sql.DB with migration support.
type DB struct {
	*sql.DB
}

// New opens (or creates) the SQLite database at path.
// Pass ":memory:" to get a fast in-memory DB (used by tests).
// WAL mode is enabled for disk-backed databases.
func New(path string) (*DB, error) {
	isMemory := path == ":memory:" || strings.Contains(path, ":memory:")

	if !isMemory {
		if dir := filepath.Dir(path); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, fmt.Errorf("create db dir %q: %w", dir, err)
			}
		}
	}

	var dsn string
	if isMemory {
		// WAL is not supported on in-memory databases.
		// MaxOpenConns=1 keeps the same connection alive, so the in-memory
		// database is visible to every query without needing cache=shared.
		dsn = "file::memory:?_foreign_keys=on&_busy_timeout=5000"
	} else {
		dsn = fmt.Sprintf(
			"file:%s?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000"+
				"&_foreign_keys=on&_cache_size=-64000",
			path,
		)
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	// Single writer connection – avoids "database is locked" under concurrent
	// reads because SQLite's WAL reader/writer separation is handled at the
	// OS level, not the connection pool level.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}

	if isMemory {
		log.Println("[db] opened in-memory database")
	} else {
		log.Printf("[db] opened %s", path)
	}
	return &DB{db}, nil
}

// RunMigrations applies all DDL statements idempotently.
func (db *DB) RunMigrations() error {
	return applyMigrations(db.DB)
}

