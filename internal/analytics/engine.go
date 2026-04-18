package analytics

import (
	"fmt"
	"log"
	"math"
	"sort"
	"time"

	"mf-analytics/internal/database"
	"mf-analytics/internal/models"
)

// Engine pre-computes and persists analytics for each fund × window.
type Engine struct {
	db *database.DB
}

// NewEngine constructs an Engine.
func NewEngine(db *database.DB) *Engine {
	return &Engine{db: db}
}

// navPoint is an internal NAV observation, sorted by date.
type navPoint struct {
	Date time.Time
	NAV  float64
}

// ── Public API ────────────────────────────────────────────────────────────────

// ComputeAll computes and stores analytics for all four windows for a scheme.
func (e *Engine) ComputeAll(schemeCode string) error {
	records, err := e.loadNAV(schemeCode)
	if err != nil {
		return fmt.Errorf("load nav %s: %w", schemeCode, err)
	}
	if len(records) < 30 {
		log.Printf("[analytics] %s: insufficient data (%d points)", schemeCode, len(records))
		return nil
	}

	scheme, err := e.loadScheme(schemeCode)
	if err != nil {
		return fmt.Errorf("load scheme %s: %w", schemeCode, err)
	}

	for _, window := range models.AllWindows {
		years, ok := models.WindowYears[window]
		if !ok {
			continue
		}
		result := e.computeWindow(records, years, window, scheme)
		if result == nil {
			continue
		}
		if err := e.saveAnalytics(result); err != nil {
			log.Printf("[analytics] save %s/%s: %v", schemeCode, window, err)
		}
	}
	log.Printf("[analytics] computed all windows for %s (%d nav points)", schemeCode, len(records))
	return nil
}

// GetAnalytics retrieves pre-computed analytics from the DB.
func (e *Engine) GetAnalytics(schemeCode, window string) (*models.Analytics, error) {
	return e.loadAnalytics(schemeCode, window)
}

// GetAllAnalyticsForCategory returns analytics for every scheme in a category,
// for a specific window.
func (e *Engine) GetAllAnalyticsForCategory(category, window string) ([]models.Analytics, error) {
	rows, err := e.db.Query(`
		SELECT a.scheme_code, s.name, s.category, s.amc,
		       a.window, a.data_start, a.data_end,
		       a.total_days, a.nav_points, a.rolling_periods,
		       a.rolling_min, a.rolling_max, a.rolling_median,
		       a.rolling_p25, a.rolling_p75,
		       a.max_drawdown,
		       a.cagr_min, a.cagr_max, a.cagr_median,
		       a.computed_at
		FROM analytics a
		JOIN schemes s ON s.code = a.scheme_code
		WHERE LOWER(s.category) LIKE '%' || LOWER(?) || '%'`,
		category, window,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.Analytics
	for rows.Next() {
		a, err := scanAnalytics(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, *a)
	}
	return results, rows.Err()
}

// ── Core computation ──────────────────────────────────────────────────────────

// computeWindow computes rolling returns, CAGR distribution, and max drawdown
// for a single window (e.g. 3 years).
func (e *Engine) computeWindow(
	records []navPoint,
	years int,
	window string,
	scheme *models.Scheme,
) *models.Analytics {

	if len(records) < 2 {
		return nil
	}

	first, last := records[0], records[len(records)-1]
	totalDays := int(last.Date.Sub(first.Date).Hours() / 24)
	minRequiredDays := int(float64(years) * 365 * 0.85) // allow 15% tolerance

	base := &models.Analytics{
		FundCode:  scheme.Code,
		FundName:  scheme.Name,
		Category:  scheme.Category,
		AMC:       scheme.AMC,
		Window:    window,
		DataStart: first.Date,
		DataEnd:   last.Date,
		TotalDays: totalDays,
		NavPoints: len(records),
		ComputedAt: time.Now().UTC(),
	}

	if totalDays < minRequiredDays {
		// Data predates window length – store a stub so the record exists
		return base
	}

	const maxGapDays = 14 // how far we search for the start-of-window NAV

	var rollingReturns []float64
	var rollingCAGRs []float64

	for i, end := range records {
		startTarget := end.Date.AddDate(-years, 0, 0)
		si := findClosest(records[:i], startTarget, maxGapDays)
		if si < 0 {
			continue
		}

		navStart := records[si].NAV
		navEnd := end.NAV
		if navStart <= 0 || navEnd <= 0 {
			continue
		}

		// Actual period in fractional years
		actualDays := end.Date.Sub(records[si].Date).Hours() / 24
		actualYears := actualDays / 365.25

		// Reject windows shorter than 90% of target
		if actualYears < float64(years)*0.9 {
			continue
		}

		ret := (navEnd/navStart - 1) * 100
		cagr := (math.Pow(navEnd/navStart, 1/actualYears) - 1) * 100

		rollingReturns = append(rollingReturns, ret)
		rollingCAGRs = append(rollingCAGRs, cagr)
	}

	if len(rollingReturns) == 0 {
		return base
	}

	sort.Float64s(rollingReturns)
	sort.Float64s(rollingCAGRs)

	base.RollingPeriods = len(rollingReturns)
	base.RollingMin = rollingReturns[0]
	base.RollingMax = rollingReturns[len(rollingReturns)-1]
	base.RollingMedian = pctile(rollingReturns, 50)
	base.RollingP25 = pctile(rollingReturns, 25)
	base.RollingP75 = pctile(rollingReturns, 75)
	base.MaxDrawdown = maxDrawdown(records)
	base.CAGRMin = rollingCAGRs[0]
	base.CAGRMax = rollingCAGRs[len(rollingCAGRs)-1]
	base.CAGRMedian = pctile(rollingCAGRs, 50)

	return base
}

// ── Algorithms ────────────────────────────────────────────────────────────────

// findClosest binary-searches sorted records for the closest date to target.
// Returns -1 if nothing is within maxGapDays calendar days.
func findClosest(records []navPoint, target time.Time, maxGapDays int) int {
	if len(records) == 0 {
		return -1
	}

	lo, hi := 0, len(records)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		if records[mid].Date.Before(target) {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}

	maxGap := time.Duration(maxGapDays) * 24 * time.Hour
	best := -1
	bestDiff := maxGap + 1

	for _, idx := range []int{lo - 1, lo} {
		if idx < 0 || idx >= len(records) {
			continue
		}
		diff := records[idx].Date.Sub(target)
		if diff < 0 {
			diff = -diff
		}
		if diff < bestDiff {
			bestDiff = diff
			best = idx
		}
	}

	if bestDiff > maxGap {
		return -1
	}
	return best
}

// maxDrawdown computes the worst peak-to-trough decline over the full series.
func maxDrawdown(records []navPoint) float64 {
	if len(records) == 0 {
		return 0
	}
	peak := records[0].NAV
	dd := 0.0
	for _, r := range records {
		if r.NAV > peak {
			peak = r.NAV
		}
		if d := (r.NAV - peak) / peak * 100; d < dd {
			dd = d
		}
	}
	return dd
}

// pctile returns the p-th percentile (0–100) of a pre-sorted slice
// using linear interpolation.
func pctile(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return sorted[0]
	}
	idx := (p / 100) * float64(n-1)
	lo := int(idx)
	hi := lo + 1
	if hi >= n {
		return sorted[n-1]
	}
	frac := idx - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac
}

// ── Database helpers ──────────────────────────────────────────────────────────

func (e *Engine) loadNAV(schemeCode string) ([]navPoint, error) {
	rows, err := e.db.Query(`
		SELECT date, nav FROM nav_records
		WHERE scheme_code = ?
		ORDER BY date ASC`,
		schemeCode,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pts []navPoint
	for rows.Next() {
		var dateStr string
		var nav float64
		if err := rows.Scan(&dateStr, &nav); err != nil {
			return nil, err
		}
		t, err := time.Parse("2006-01-02", dateStr)
		if err != nil {
			continue
		}
		pts = append(pts, navPoint{Date: t, NAV: nav})
	}
	return pts, rows.Err()
}

func (e *Engine) loadScheme(code string) (*models.Scheme, error) {
	var s models.Scheme
	err := e.db.QueryRow(
		`SELECT code, name, amc, category FROM schemes WHERE code = ?`, code,
	).Scan(&s.Code, &s.Name, &s.AMC, &s.Category)
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func (e *Engine) saveAnalytics(a *models.Analytics) error {
	var startStr, endStr, computedStr string
	if !a.DataStart.IsZero() {
		startStr = a.DataStart.Format("2006-01-02")
	}
	if !a.DataEnd.IsZero() {
		endStr = a.DataEnd.Format("2006-01-02")
	}
	computedStr = a.ComputedAt.UTC().Format(time.RFC3339)

	_, err := e.db.Exec(`
		INSERT INTO analytics (
			scheme_code, window,
			data_start, data_end, total_days, nav_points,
			rolling_periods, rolling_min, rolling_max, rolling_median, rolling_p25, rolling_p75,
			max_drawdown,
			cagr_min, cagr_max, cagr_median,
			computed_at
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(scheme_code, window) DO UPDATE SET
			data_start      = excluded.data_start,
			data_end        = excluded.data_end,
			total_days      = excluded.total_days,
			nav_points      = excluded.nav_points,
			rolling_periods = excluded.rolling_periods,
			rolling_min     = excluded.rolling_min,
			rolling_max     = excluded.rolling_max,
			rolling_median  = excluded.rolling_median,
			rolling_p25     = excluded.rolling_p25,
			rolling_p75     = excluded.rolling_p75,
			max_drawdown    = excluded.max_drawdown,
			cagr_min        = excluded.cagr_min,
			cagr_max        = excluded.cagr_max,
			cagr_median     = excluded.cagr_median,
			computed_at     = excluded.computed_at`,
		a.FundCode, a.Window,
		startStr, endStr, a.TotalDays, a.NavPoints,
		a.RollingPeriods, a.RollingMin, a.RollingMax, a.RollingMedian, a.RollingP25, a.RollingP75,
		a.MaxDrawdown,
		a.CAGRMin, a.CAGRMax, a.CAGRMedian,
		computedStr,
	)
	return err
}

func (e *Engine) loadAnalytics(schemeCode, window string) (*models.Analytics, error) {
	row := e.db.QueryRow(`
		SELECT a.scheme_code, s.name, s.category, s.amc,
		       a.window, a.data_start, a.data_end,
		       a.total_days, a.nav_points, a.rolling_periods,
		       a.rolling_min, a.rolling_max, a.rolling_median,
		       a.rolling_p25, a.rolling_p75,
		       a.max_drawdown,
		       a.cagr_min, a.cagr_max, a.cagr_median,
		       a.computed_at
		FROM analytics a
		JOIN schemes s ON s.code = a.scheme_code
		WHERE a.scheme_code = ? AND a.window = ?`,
		schemeCode, window,
	)
	return scanAnalytics(row)
}

// scanner is satisfied by both *sql.Row and *sql.Rows
type scanner interface {
	Scan(dest ...any) error
}

func scanAnalytics(row scanner) (*models.Analytics, error) {
	var a models.Analytics
	var dataStart, dataEnd, computedAt string
	err := row.Scan(
		&a.FundCode, &a.FundName, &a.Category, &a.AMC,
		&a.Window, &dataStart, &dataEnd,
		&a.TotalDays, &a.NavPoints, &a.RollingPeriods,
		&a.RollingMin, &a.RollingMax, &a.RollingMedian,
		&a.RollingP25, &a.RollingP75,
		&a.MaxDrawdown,
		&a.CAGRMin, &a.CAGRMax, &a.CAGRMedian,
		&computedAt,
	)
	if err != nil {
		return nil, err
	}
	a.DataStart, _ = time.Parse("2006-01-02", dataStart)
	a.DataEnd, _ = time.Parse("2006-01-02", dataEnd)
	a.ComputedAt, _ = time.Parse(time.RFC3339, computedAt)
	return &a, nil
}

