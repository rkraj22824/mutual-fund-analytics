package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"mf-analytics/internal/analytics"
	"mf-analytics/internal/database"
	"mf-analytics/internal/ingestion"
	"mf-analytics/internal/models"
)

// Handlers holds all HTTP handler dependencies.
type Handlers struct {
	db        *database.DB
	pipeline  PipelineSyncer // interface defined in router.go
	analytics *analytics.Engine
	cache     *Cache
}

// ─────────────────────────────────────────────────────────────────────────────
// Core helpers
// ─────────────────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func validWindow(w string) bool {
	_, ok := models.WindowYears[w]
	return ok
}

// cachedResponse is the central cache-aware response helper.
//
//   - Cache HIT  → write raw bytes, set X-Cache: HIT header.
//   - Cache MISS → call compute(), serialise, cache, write to w.
func (h *Handlers) cachedResponse(
	w http.ResponseWriter,
	key string,
	ttl time.Duration,
	compute func() (any, int, error),
) {
	if data, ok := h.cache.Get(key); ok {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "HIT")
		w.Write(data)
		return
	}

	v, statusCode, err := compute()
	if err != nil {
		writeError(w, statusCode, err.Error())
		return
	}

	data, encErr := json.Marshal(v)
	if encErr != nil {
		writeError(w, http.StatusInternalServerError, "serialisation error")
		return
	}

	h.cache.Set(key, data, ttl)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	w.WriteHeader(statusCode)
	w.Write(data)
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /health
// ─────────────────────────────────────────────────────────────────────────────

func (h *Handlers) handleHealth(w http.ResponseWriter, r *http.Request) {
	hits, misses, size := h.cache.Stats()
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		"time":   time.Now().UTC().Format(time.RFC3339),
		"cache": map[string]any{
			"entries": size,
			"hits":    hits,
			"misses":  misses,
		},
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /funds   ?category=  ?amc=
//
// category is matched with LIKE so partial values work:
//   "Equity"           → matches "Equity: Mid Cap" AND "Equity: Small Cap"
//   "Mid Cap"          → matches "Equity: Mid Cap"
//   "Equity: Mid Cap"  → exact match
// ─────────────────────────────────────────────────────────────────────────────

func (h *Handlers) handleListFunds(w http.ResponseWriter, r *http.Request) {
	category := r.URL.Query().Get("category")
	amc := r.URL.Query().Get("amc")

	cacheKey := fmt.Sprintf("funds:list:%s:%s",
		strings.ToLower(category), strings.ToLower(amc))

	h.cachedResponse(w, cacheKey, ttlFundList, func() (any, int, error) {
		schemes, err := ingestion.LoadSchemesFromDB(h.db, category, amc)
		if err != nil {
			return nil, http.StatusInternalServerError,
				fmt.Errorf("load funds: %w", err)
		}
		return map[string]any{
			"total": len(schemes),
			"funds": schemes,
		}, http.StatusOK, nil
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /funds/{code}
// ─────────────────────────────────────────────────────────────────────────────

func (h *Handlers) handleGetFund(w http.ResponseWriter, r *http.Request) {
	code := chi.URLParam(r, "code")
	if code == "" {
		writeError(w, http.StatusBadRequest, "missing fund code")
		return
	}

	cacheKey := fmt.Sprintf("fund:%s", code)

	h.cachedResponse(w, cacheKey, ttlFundDetail, func() (any, int, error) {
		var s models.Scheme
		var lastSyncRaw sql.NullString
		var createdRaw string

		err := h.db.QueryRow(`
			SELECT code, name, amc, category, last_synced_at, created_at
			FROM   schemes
			WHERE  code = ?`, code,
		).Scan(&s.Code, &s.Name, &s.AMC, &s.Category, &lastSyncRaw, &createdRaw)

		if err == sql.ErrNoRows {
			return nil, http.StatusNotFound,
				fmt.Errorf("fund %q not found", code)
		}
		if err != nil {
			return nil, http.StatusInternalServerError,
				fmt.Errorf("database error: %w", err)
		}

		if lastSyncRaw.Valid {
			t, _ := time.Parse(time.RFC3339, lastSyncRaw.String)
			s.LastSyncAt = &t
		}
		s.CreatedAt, _ = time.Parse(time.RFC3339, createdRaw)

		// Fetch latest NAV and total record count in a single query.
		var latestDate string
		var latestNAV float64
		var navCount int
		h.db.QueryRow(`
			SELECT
			    COALESCE((SELECT nav  FROM nav_records
			              WHERE scheme_code = ?
			              ORDER BY date DESC LIMIT 1), 0),
			    COALESCE((SELECT date FROM nav_records
			              WHERE scheme_code = ?
			              ORDER BY date DESC LIMIT 1), ''),
			    (SELECT COUNT(*) FROM nav_records WHERE scheme_code = ?)`,
			code, code, code,
		).Scan(&latestNAV, &latestDate, &navCount)

		return map[string]any{
			"fund":              s,
			"latest_nav":        latestNAV,
			"latest_date":       latestDate,
			"total_nav_records": navCount,
		}, http.StatusOK, nil
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /funds/{code}/analytics   ?window=1Y|3Y|5Y|10Y  (required)
// ─────────────────────────────────────────────────────────────────────────────

type analyticsResponse struct {
	FundCode string `json:"fund_code"`
	FundName string `json:"fund_name"`
	Category string `json:"category"`
	AMC      string `json:"amc"`
	Window   string `json:"window"`

	DataAvailability struct {
		StartDate string `json:"start_date"`
		EndDate   string `json:"end_date"`
		TotalDays int    `json:"total_days"`
		NavPoints int    `json:"nav_data_points"`
	} `json:"data_availability"`

	RollingPeriodsAnalyzed int `json:"rolling_periods_analyzed"`

	RollingReturns struct {
		Min    float64 `json:"min"`
		Max    float64 `json:"max"`
		Median float64 `json:"median"`
		P25    float64 `json:"p25"`
		P75    float64 `json:"p75"`
	} `json:"rolling_returns"`

	MaxDrawdown float64 `json:"max_drawdown"`

	CAGR struct {
		Min    float64 `json:"min"`
		Max    float64 `json:"max"`
		Median float64 `json:"median"`
	} `json:"cagr"`

	ComputedAt string `json:"computed_at"`
}

func (h *Handlers) handleGetAnalytics(w http.ResponseWriter, r *http.Request) {
	code := chi.URLParam(r, "code")
	window := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("window")))

	if !validWindow(window) {
		writeError(w, http.StatusBadRequest,
			"window is required; must be one of: 1Y, 3Y, 5Y, 10Y")
		return
	}

	cacheKey := fmt.Sprintf("analytics:%s:%s", code, window)

	h.cachedResponse(w, cacheKey, ttlAnalytics, func() (any, int, error) {
		a, err := h.analytics.GetAnalytics(code, window)
		if err == sql.ErrNoRows {
			return nil, http.StatusNotFound, fmt.Errorf(
				"analytics for %s/%s not yet computed – POST /sync/trigger first",
				code, window)
		}
		if err != nil {
			return nil, http.StatusInternalServerError,
				fmt.Errorf("analytics lookup: %w", err)
		}

		resp := analyticsResponse{
			FundCode:               a.FundCode,
			FundName:               a.FundName,
			Category:               a.Category,
			AMC:                    a.AMC,
			Window:                 a.Window,
			RollingPeriodsAnalyzed: a.RollingPeriods,
			MaxDrawdown:            round2(a.MaxDrawdown),
			ComputedAt:             a.ComputedAt.UTC().Format(time.RFC3339),
		}
		resp.DataAvailability.StartDate = a.DataStart.Format("2006-01-02")
		resp.DataAvailability.EndDate = a.DataEnd.Format("2006-01-02")
		resp.DataAvailability.TotalDays = a.TotalDays
		resp.DataAvailability.NavPoints = a.NavPoints
		resp.RollingReturns.Min = round2(a.RollingMin)
		resp.RollingReturns.Max = round2(a.RollingMax)
		resp.RollingReturns.Median = round2(a.RollingMedian)
		resp.RollingReturns.P25 = round2(a.RollingP25)
		resp.RollingReturns.P75 = round2(a.RollingP75)
		resp.CAGR.Min = round2(a.CAGRMin)
		resp.CAGR.Max = round2(a.CAGRMax)
		resp.CAGR.Median = round2(a.CAGRMedian)

		return resp, http.StatusOK, nil
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /funds/rank
//   ?category=  (required)   – LIKE matched; "Equity" returns all equity funds
//   ?window=    (required)   – 1Y|3Y|5Y|10Y
//   ?sort_by=               – median_return (default) | max_drawdown
//   ?limit=                 – default 5
// ─────────────────────────────────────────────────────────────────────────────

type rankEntry struct {
	Rank         int     `json:"rank"`
	FundCode     string  `json:"fund_code"`
	FundName     string  `json:"fund_name"`
	AMC          string  `json:"amc"`
	MedianReturn float64 `json:"median_return"`
	MaxDrawdown  float64 `json:"max_drawdown"`
	CurrentNAV   float64 `json:"current_nav"`
	LastUpdated  string  `json:"last_updated"`
}

type rankResponse struct {
	Category   string      `json:"category"`
	Window     string      `json:"window"`
	SortedBy   string      `json:"sorted_by"`
	TotalFunds int         `json:"total_funds"`
	Showing    int         `json:"showing"`
	Funds      []rankEntry `json:"funds"`
}

func (h *Handlers) handleRankFunds(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	category := strings.TrimSpace(q.Get("category"))
	window := strings.ToUpper(strings.TrimSpace(q.Get("window")))
	sortBy := strings.TrimSpace(q.Get("sort_by"))
	limitStr := q.Get("limit")

	if category == "" {
		writeError(w, http.StatusBadRequest,
			`category is required (e.g. ?category=Equity, ?category=Equity%3A+Mid+Cap)`)
		return
	}
	if !validWindow(window) {
		writeError(w, http.StatusBadRequest,
			"window is required; must be one of: 1Y, 3Y, 5Y, 10Y")
		return
	}
	if sortBy == "" {
		sortBy = "median_return"
	}
	if sortBy != "median_return" && sortBy != "max_drawdown" {
		writeError(w, http.StatusBadRequest,
			"sort_by must be median_return or max_drawdown")
		return
	}
	limit := 5
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	cacheKey := fmt.Sprintf("rank:%s:%s:%s:%d",
		strings.ToLower(category), window, sortBy, limit)

	h.cachedResponse(w, cacheKey, ttlRanking, func() (any, int, error) {
		entries, total, err := h.queryRanking(category, window, sortBy, limit)
		if err != nil {
			return nil, http.StatusInternalServerError,
				fmt.Errorf("ranking query: %w", err)
		}

		if entries == nil {
			entries = make([]rankEntry, 0) // never null in JSON
		}

		// Helpful diagnostic when the table is empty.
		msg := ""
		if total == 0 {
			msg = fmt.Sprintf(
				"no analytics found for category=%q window=%s – "+
					"POST /sync/trigger and wait for backfill to complete",
				category, window)
		}
		_ = msg // included in response body only when total==0

		resp := rankResponse{
			Category:   category,
			Window:     window,
			SortedBy:   sortBy,
			TotalFunds: total,
			Showing:    len(entries),
			Funds:      entries,
		}
		if total == 0 {
			// Return 200 with the empty list plus a hint.
			return map[string]any{
				"category":    resp.Category,
				"window":      resp.Window,
				"sorted_by":   resp.SortedBy,
				"total_funds": resp.TotalFunds,
				"showing":     resp.Showing,
				"funds":       resp.Funds,
				"hint":        msg,
			}, http.StatusOK, nil
		}
		return resp, http.StatusOK, nil
	})
}

// queryRanking executes a single SQL query that:
//   - Filters by category using LIKE (so "Equity" matches all equity categories)
//   - Fetches current NAV via correlated sub-queries (avoids N+1)
//   - Sorts in SQL (avoids a Go sort on large result sets)
//   - Uses COUNT(*) OVER() to return total_funds in the same round-trip
func (h *Handlers) queryRanking(
	category, window, sortBy string, limit int,
) ([]rankEntry, int, error) {

	orderClause := "a.rolling_median DESC"
	if sortBy == "max_drawdown" {
		orderClause = "a.max_drawdown DESC"
	}

	// We intentionally embed the sort direction via Go string formatting
	// rather than a parameter because SQL does not allow ORDER BY to be
	// parameterised. The only two accepted values ("rolling_median DESC" and
	// "max_drawdown DESC") are validated above, so there is no injection risk.
	query := fmt.Sprintf(`
		SELECT
		    a.scheme_code,
		    s.name,
		    s.amc,
		    a.rolling_median,
		    a.max_drawdown,
		    COALESCE(
		        (SELECT nav FROM nav_records
		         WHERE scheme_code = a.scheme_code
		         ORDER BY date DESC LIMIT 1),
		        0.0
		    ) AS current_nav,
		    COALESCE(
		        (SELECT date FROM nav_records
		         WHERE scheme_code = a.scheme_code
		         ORDER BY date DESC LIMIT 1),
		        ''
		    ) AS latest_date,
		    COUNT(*) OVER () AS total_count
		FROM  analytics a
		JOIN  schemes s ON s.code = a.scheme_code
		WHERE LOWER(s.category) LIKE '%%' || LOWER(?) || '%%'
		  AND a.window = ?
		ORDER BY %s
		LIMIT ?`, orderClause)

	rows, err := h.db.Query(query, category, window, limit)
	if err != nil {
		return nil, 0, fmt.Errorf("queryRanking: %w", err)
	}
	defer rows.Close()

	entries := make([]rankEntry, 0, limit) // initialise – never serialise as null
	total := 0
	rank := 1

	for rows.Next() {
		var e rankEntry
		var rollingMedian, maxDrawdown, currentNAV float64
		var latestDate string

		if err := rows.Scan(
			&e.FundCode, &e.FundName, &e.AMC,
			&rollingMedian, &maxDrawdown,
			&currentNAV, &latestDate,
			&total,
		); err != nil {
			return nil, 0, fmt.Errorf("queryRanking scan: %w", err)
		}

		e.Rank = rank
		e.MedianReturn = round2(rollingMedian)
		e.MaxDrawdown = round2(maxDrawdown)
		e.CurrentNAV = currentNAV
		e.LastUpdated = latestDate
		entries = append(entries, e)
		rank++
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	return entries, total, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// POST /sync/trigger
// ─────────────────────────────────────────────────────────────────────────────

func (h *Handlers) handleSyncTrigger(w http.ResponseWriter, r *http.Request) {
	if err := h.pipeline.TriggerSync(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError,
			fmt.Sprintf("trigger sync: %v", err))
		return
	}

	// Invalidate all caches so the next read reflects freshly ingested data.
	h.cache.InvalidatePrefix("analytics:")
	h.cache.InvalidatePrefix("rank:")
	h.cache.InvalidatePrefix("funds:")
	h.cache.InvalidatePrefix("fund:")

	writeJSON(w, http.StatusAccepted, map[string]string{
		"status":  "accepted",
		"message": "incremental sync jobs enqueued; caches invalidated",
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /sync/status
// ─────────────────────────────────────────────────────────────────────────────

func (h *Handlers) handleSyncStatus(w http.ResponseWriter, r *http.Request) {
	status, err := h.pipeline.Status()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "pipeline status unavailable")
		return
	}
	writeJSON(w, http.StatusOK, status)
}

// ─────────────────────────────────────────────────────────────────────────────
// Utilities
// ─────────────────────────────────────────────────────────────────────────────

func round2(v float64) float64 {
	return float64(int64(v*100+0.5)) / 100
}

