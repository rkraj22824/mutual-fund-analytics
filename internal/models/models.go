package models

import "time"

// ── Constants ────────────────────────────────────────────────────────────────

const (
	// Full canonical category names that mirror mfapi.in scheme_category groupings.
	// Handlers use LIKE matching, so queries for "Equity", "Mid Cap",
	// "Equity: Mid Cap", etc. all resolve correctly.
	CategoryMidCap   = "Equity: Mid Cap"
	CategorySmallCap = "Equity: Small Cap"

	Window1Y  = "1Y"
	Window3Y  = "3Y"
	Window5Y  = "5Y"
	Window10Y = "10Y"

	JobTypeBackfill    = "backfill"
	JobTypeIncremental = "incremental"

	JobPending   = "pending"
	JobRunning   = "running"
	JobCompleted = "completed"
	JobFailed    = "failed"
)

// AllWindows lists the four analytics windows in order.
var AllWindows = []string{Window1Y, Window3Y, Window5Y, Window10Y}

// WindowYears maps window code → integer years.
var WindowYears = map[string]int{
	Window1Y:  1,
	Window3Y:  3,
	Window5Y:  5,
	Window10Y: 10,
}

// ── Domain models ─────────────────────────────────────────────────────────────

// Scheme represents a tracked mutual fund.
type Scheme struct {
	Code       string     `json:"code"`
	Name       string     `json:"name"`
	AMC        string     `json:"amc"`
	Category   string     `json:"category"`
	LastSyncAt *time.Time `json:"last_synced_at,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
}

// NAVPoint is a single NAV observation.
type NAVPoint struct {
	SchemeCode string
	Date       time.Time
	NAV        float64
}

// Analytics holds pre-computed analytics for one fund × one window.
type Analytics struct {
	FundCode  string    `json:"fund_code"`
	FundName  string    `json:"fund_name"`
	Category  string    `json:"category"`
	AMC       string    `json:"amc"`
	Window    string    `json:"window"`
	DataStart time.Time `json:"data_start"`
	DataEnd   time.Time `json:"data_end"`
	TotalDays int       `json:"total_days"`
	NavPoints int       `json:"nav_points"`

	RollingPeriods int     `json:"rolling_periods_analyzed"`
	RollingMin     float64 `json:"rolling_min"`
	RollingMax     float64 `json:"rolling_max"`
	RollingMedian  float64 `json:"rolling_median"`
	RollingP25     float64 `json:"rolling_p25"`
	RollingP75     float64 `json:"rolling_p75"`

	MaxDrawdown float64 `json:"max_drawdown"`

	CAGRMin    float64 `json:"cagr_min"`
	CAGRMax    float64 `json:"cagr_max"`
	CAGRMedian float64 `json:"cagr_median"`

	ComputedAt time.Time `json:"computed_at"`
}

// SyncJob is a single data-fetch task in the pipeline queue.
type SyncJob struct {
	ID         int64
	SchemeCode string
	JobType    string
	Status     string
	ErrorMsg   string
	RetryCount int
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// PipelineStatus is returned by GET /sync/status.
type PipelineStatus struct {
	Status     string          `json:"status"`
	Total      int             `json:"total_schemes"`
	Completed  int             `json:"completed"`
	InProgress int             `json:"in_progress"`
	Failed     int             `json:"failed"`
	Pending    int             `json:"pending"`
	RateLimit  RateLimitStatus `json:"rate_limit"`
	LastSyncAt *time.Time      `json:"last_sync_at,omitempty"`
	Message    string          `json:"message"`
}

// RateLimitStatus shows live rate-limit consumption.
type RateLimitStatus struct {
	UsedLastSecond int `json:"used_last_second"`
	UsedLastMinute int `json:"used_last_minute"`
	UsedLastHour   int `json:"used_last_hour"`
	FreePerSecond  int `json:"free_per_second"`
	FreePerMinute  int `json:"free_per_minute"`
	FreePerHour    int `json:"free_per_hour"`
}

