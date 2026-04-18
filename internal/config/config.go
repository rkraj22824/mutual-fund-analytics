package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds all runtime configuration, sourced from env vars with defaults.
type Config struct {
	ServerPort   string
	DatabasePath string
	MFAPIBaseURL string

	// Strict API rate limits – ALL three enforced simultaneously
	RatePerSecond int
	RatePerMinute int
	RatePerHour   int

	// Pipeline
	SyncInterval  time.Duration
	BackfillYears int
}

func Load() *Config {
	return &Config{
		ServerPort:    env("SERVER_PORT", "8080"),
		DatabasePath:  env("DATABASE_PATH", "./data/mf_analytics.db"),
		MFAPIBaseURL:  env("MFAPI_BASE_URL", "https://api.mfapi.in"),
		RatePerSecond: envInt("RATE_PER_SECOND", 2),
		RatePerMinute: envInt("RATE_PER_MINUTE", 50),
		RatePerHour:   envInt("RATE_PER_HOUR", 300),
		SyncInterval:  time.Duration(envInt("SYNC_INTERVAL_HOURS", 24)) * time.Hour,
		BackfillYears: envInt("BACKFILL_YEARS", 10),
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}

