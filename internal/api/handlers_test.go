package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"mf-analytics/internal/analytics"
	"mf-analytics/internal/api"
	"mf-analytics/internal/database"
	"mf-analytics/internal/models"
)

// ─────────────────────────────────────────────────────────────────────────────
// Pipeline stub
// ─────────────────────────────────────────────────────────────────────────────

// stubPipeline implements api.PipelineSyncer without any real pipeline logic.
type stubPipeline struct{}

func (s *stubPipeline) TriggerSync(_ context.Context) error {
	return nil
}
func (s *stubPipeline) Status() (models.PipelineStatus, error) {
	return models.PipelineStatus{Status: "idle", Total: 10}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Test server + fixture data
// ─────────────────────────────────────────────────────────────────────────────

func testServer(tb testing.TB) *httptest.Server {
	tb.Helper()

	db, err := database.New(":memory:")
	if err != nil {
		tb.Fatalf("testServer db: %v", err)
	}
	if err := db.RunMigrations(); err != nil {
		tb.Fatalf("testServer migrations: %v", err)
	}

	seedTestData(tb, db)

	engine := analytics.NewEngine(db)
	router := api.NewRouter(db, &stubPipeline{}, engine)
	srv := httptest.NewServer(router)

	tb.Cleanup(func() {
		srv.Close()
		db.Close()
	})
	return srv
}

// seedTestData inserts 10 schemes, 4-window analytics, and 30 NAV records each.
// Categories use the canonical "Equity: Mid Cap" / "Equity: Small Cap" naming.
func seedTestData(tb testing.TB, db *database.DB) {
	tb.Helper()

	type schemeRow struct {
		code, name, amc, category string
	}
	schemes := []schemeRow{
		{"120505", "ICICI Pru Midcap Direct Growth", "ICICI Prudential Mutual Fund", "Equity: Mid Cap"},
		{"120841", "ICICI Pru Smallcap Direct Growth", "ICICI Prudential Mutual Fund", "Equity: Small Cap"},
		{"118989", "HDFC Mid-Cap Opportunities Direct Growth", "HDFC Mutual Fund", "Equity: Mid Cap"},
		{"125354", "HDFC Small Cap Direct Growth", "HDFC Mutual Fund", "Equity: Small Cap"},
		{"119598", "Axis Midcap Direct Growth", "Axis Mutual Fund", "Equity: Mid Cap"},
		{"125494", "Axis Small Cap Direct Growth", "Axis Mutual Fund", "Equity: Small Cap"},
		{"119775", "SBI Magnum Midcap Direct Growth", "SBI Mutual Fund", "Equity: Mid Cap"},
		{"125349", "SBI Small Cap Direct Growth", "SBI Mutual Fund", "Equity: Small Cap"},
		{"119007", "Kotak Emerging Equity Direct Growth", "Kotak Mahindra Mutual Fund", "Equity: Mid Cap"},
		{"120594", "Kotak Small Cap Direct Growth", "Kotak Mahindra Mutual Fund", "Equity: Small Cap"},
	}

	now := time.Now()

	for i, s := range schemes {
		if _, err := db.Exec(
			`INSERT INTO schemes (code, name, amc, category)
			 VALUES (?,?,?,?) ON CONFLICT(code) DO UPDATE SET
			   name=excluded.name, amc=excluded.amc, category=excluded.category`,
			s.code, s.name, s.amc, s.category,
		); err != nil {
			tb.Fatalf("seed scheme %s: %v", s.code, err)
		}

		// 30 daily NAV records, most-recent first.
		for d := 0; d < 30; d++ {
			date := now.AddDate(0, 0, -d).Format("2006-01-02")
			db.Exec(
				`INSERT OR IGNORE INTO nav_records (scheme_code, date, nav)
				 VALUES (?,?,?)`,
				s.code, date, 80.0+float64(i)+float64(d)*0.1,
			)
		}

		// Analytics for all four windows with slightly varying medians.
		medianBase := 18.0 + float64(i)*0.8 // ensures deterministic sort order
		for _, w := range models.AllWindows {
			db.Exec(`
				INSERT INTO analytics (
					scheme_code, window,
					data_start, data_end, total_days, nav_points,
					rolling_periods,
					rolling_min, rolling_max, rolling_median, rolling_p25, rolling_p75,
					max_drawdown,
					cagr_min, cagr_max, cagr_median,
					computed_at
				) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
				ON CONFLICT(scheme_code, window) DO UPDATE SET
					rolling_median = excluded.rolling_median,
					max_drawdown   = excluded.max_drawdown,
					computed_at    = excluded.computed_at`,
				s.code, w,
				"2016-01-01", now.Format("2006-01-02"),
				3000, 2000,
				730,
				8.5, 45.2, medianBase, 15.7, 28.9,
				-30.0-float64(i%5),
				9.5, 44.0, medianBase-0.5,
				now.UTC().Format(time.RFC3339),
			)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

const responseTimeBudget = 200 * time.Millisecond

// assertFastResponse sends GET path, checks status, asserts response time.
func assertFastResponse(t *testing.T, srv *httptest.Server, path string, wantStatus int) *http.Response {
	t.Helper()

	start := time.Now()
	resp, err := http.Get(srv.URL + path)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}

	if elapsed > responseTimeBudget {
		t.Errorf("GET %s: response time %s exceeds %s budget",
			path, elapsed.Round(time.Millisecond), responseTimeBudget)
	}
	if resp.StatusCode != wantStatus {
		t.Errorf("GET %s: want HTTP %d, got %d", path, wantStatus, resp.StatusCode)
	}
	return resp
}

func decodeJSON(t *testing.T, resp *http.Response, dst any) {
	t.Helper()
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(dst); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Response-time tests
// ─────────────────────────────────────────────────────────────────────────────

func TestResponseTime_ListFunds(t *testing.T) {
	srv := testServer(t)
	for _, path := range []string{
		"/funds",
		"/funds?category=Equity",
		"/funds?category=Equity%3A+Mid+Cap",
		"/funds?category=Mid+Cap",
		"/funds?amc=HDFC",
	} {
		resp := assertFastResponse(t, srv, path, http.StatusOK)
		resp.Body.Close()
	}
}

func TestResponseTime_GetFund(t *testing.T) {
	srv := testServer(t)
	resp := assertFastResponse(t, srv, "/funds/119598", http.StatusOK)
	resp.Body.Close()
}

func TestResponseTime_Analytics(t *testing.T) {
	srv := testServer(t)
	for _, w := range []string{"1Y", "3Y", "5Y", "10Y"} {
		resp := assertFastResponse(t, srv,
			"/funds/119598/analytics?window="+w, http.StatusOK)
		resp.Body.Close()
	}
}

func TestResponseTime_Rank(t *testing.T) {
	srv := testServer(t)
	for _, tc := range []string{
		"/funds/rank?category=Equity&window=3Y",
		"/funds/rank?category=Equity%3A+Mid+Cap&window=5Y",
		"/funds/rank?category=Equity%3A+Small+Cap&window=1Y&sort_by=max_drawdown",
		"/funds/rank?category=Mid+Cap&window=10Y&limit=3",
	} {
		resp := assertFastResponse(t, srv, tc, http.StatusOK)
		resp.Body.Close()
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Category matching correctness
// ─────────────────────────────────────────────────────────────────────────────

func TestRank_CategoryEquityMatchesBoth(t *testing.T) {
	srv := testServer(t)

	resp := assertFastResponse(t, srv,
		"/funds/rank?category=Equity&window=3Y&limit=10", http.StatusOK)

	var result struct {
		TotalFunds int         `json:"total_funds"`
		Funds      []rankEntry `json:"funds"`
	}
	decodeJSON(t, resp, &result)

	// "Equity" must match both Mid Cap (5 funds) and Small Cap (5 funds) = 10.
	if result.TotalFunds != 10 {
		t.Errorf("category=Equity: want 10 total_funds, got %d", result.TotalFunds)
	}
	if len(result.Funds) == 0 {
		t.Error("funds array must not be empty when analytics exist")
	}
}

func TestRank_CategoryMidCapOnly(t *testing.T) {
	srv := testServer(t)

	resp := assertFastResponse(t, srv,
		"/funds/rank?category=Mid+Cap&window=3Y&limit=10", http.StatusOK)

	var result struct {
		TotalFunds int         `json:"total_funds"`
		Funds      []rankEntry `json:"funds"`
	}
	decodeJSON(t, resp, &result)

	if result.TotalFunds != 5 {
		t.Errorf("category=Mid Cap: want 5 total_funds, got %d", result.TotalFunds)
	}
}

func TestRank_CategorySmallCapOnly(t *testing.T) {
	srv := testServer(t)

	resp := assertFastResponse(t, srv,
		"/funds/rank?category=Small+Cap&window=3Y&limit=10", http.StatusOK)

	var result struct {
		TotalFunds int `json:"total_funds"`
	}
	decodeJSON(t, resp, &result)

	if result.TotalFunds != 5 {
		t.Errorf("category=Small Cap: want 5 total_funds, got %d", result.TotalFunds)
	}
}

func TestRank_CategoryExactMatch(t *testing.T) {
	srv := testServer(t)

	resp := assertFastResponse(t, srv,
		"/funds/rank?category=Equity%3A+Mid+Cap&window=3Y&limit=10", http.StatusOK)

	var result struct {
		TotalFunds int `json:"total_funds"`
	}
	decodeJSON(t, resp, &result)

	if result.TotalFunds != 5 {
		t.Errorf("category='Equity: Mid Cap': want 5, got %d", result.TotalFunds)
	}
}

// rankEntry mirrors the JSON shape; used by decodeJSON above.
type rankEntry struct {
	Rank         int     `json:"rank"`
	FundCode     string  `json:"fund_code"`
	MedianReturn float64 `json:"median_return"`
	MaxDrawdown  float64 `json:"max_drawdown"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Funds array is never null
// ─────────────────────────────────────────────────────────────────────────────

func TestRank_FundsNeverNull(t *testing.T) {
	srv := testServer(t)

	// Query a category/window combination that has no data.
	resp := assertFastResponse(t, srv,
		"/funds/rank?category=NonExistentCategory&window=3Y", http.StatusOK)
	defer resp.Body.Close()

	var raw map[string]json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		t.Fatalf("decode: %v", err)
	}

	fundsRaw, ok := raw["funds"]
	if !ok {
		t.Fatal(`response missing "funds" key`)
	}
	if string(fundsRaw) == "null" {
		t.Error(`"funds" must never be JSON null; expected []`)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Sort order correctness
// ─────────────────────────────────────────────────────────────────────────────

func TestRank_SortedByMedianReturn(t *testing.T) {
	srv := testServer(t)

	resp := assertFastResponse(t, srv,
		"/funds/rank?category=Equity&window=3Y&limit=10", http.StatusOK)

	var result struct {
		Funds []struct {
			Rank         int     `json:"rank"`
			MedianReturn float64 `json:"median_return"`
		} `json:"funds"`
	}
	decodeJSON(t, resp, &result)

	for i := 1; i < len(result.Funds); i++ {
		if result.Funds[i].MedianReturn > result.Funds[i-1].MedianReturn {
			t.Errorf(
				"rank %d (median=%.2f) > rank %d (median=%.2f): order is wrong",
				result.Funds[i].Rank, result.Funds[i].MedianReturn,
				result.Funds[i-1].Rank, result.Funds[i-1].MedianReturn,
			)
		}
		if result.Funds[i].Rank != i+1 {
			t.Errorf("fund[%d].rank: want %d, got %d",
				i, i+1, result.Funds[i].Rank)
		}
	}
}

func TestRank_SortedByMaxDrawdown(t *testing.T) {
	srv := testServer(t)

	resp := assertFastResponse(t, srv,
		"/funds/rank?category=Equity&window=3Y&sort_by=max_drawdown&limit=10",
		http.StatusOK)

	var result struct {
		Funds []struct {
			MaxDrawdown float64 `json:"max_drawdown"`
		} `json:"funds"`
	}
	decodeJSON(t, resp, &result)

	for i := 1; i < len(result.Funds); i++ {
		if result.Funds[i].MaxDrawdown > result.Funds[i-1].MaxDrawdown {
			t.Errorf(
				"fund[%d].max_drawdown (%.2f) > fund[%d].max_drawdown (%.2f): "+
					"less-negative should rank higher",
				i, result.Funds[i].MaxDrawdown,
				i-1, result.Funds[i-1].MaxDrawdown,
			)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Validation errors
// ─────────────────────────────────────────────────────────────────────────────

func TestRank_MissingCategory_Returns400(t *testing.T) {
	srv := testServer(t)
	resp := assertFastResponse(t, srv, "/funds/rank?window=3Y", http.StatusBadRequest)
	resp.Body.Close()
}

func TestRank_MissingWindow_Returns400(t *testing.T) {
	srv := testServer(t)
	resp := assertFastResponse(t, srv, "/funds/rank?category=Equity", http.StatusBadRequest)
	resp.Body.Close()
}

func TestRank_InvalidWindow_Returns400(t *testing.T) {
	srv := testServer(t)
	resp := assertFastResponse(t, srv,
		"/funds/rank?category=Equity&window=2Y", http.StatusBadRequest)
	resp.Body.Close()
}

func TestAnalytics_InvalidWindow_Returns400(t *testing.T) {
	srv := testServer(t)
	resp := assertFastResponse(t, srv,
		"/funds/119598/analytics?window=2Y", http.StatusBadRequest)
	resp.Body.Close()
}

func TestGetFund_NotFound(t *testing.T) {
	srv := testServer(t)
	resp := assertFastResponse(t, srv, "/funds/999999", http.StatusNotFound)
	resp.Body.Close()
}

// ─────────────────────────────────────────────────────────────────────────────
// Limit is respected
// ─────────────────────────────────────────────────────────────────────────────

func TestRank_LimitRespected(t *testing.T) {
	srv := testServer(t)
	for _, limit := range []int{1, 3, 5} {
		resp := assertFastResponse(t, srv,
			fmt.Sprintf("/funds/rank?category=Equity&window=3Y&limit=%d", limit),
			http.StatusOK)
		var result struct {
			Showing int         `json:"showing"`
			Funds   []rankEntry `json:"funds"`
		}
		decodeJSON(t, resp, &result)
		if len(result.Funds) > limit {
			t.Errorf("limit=%d: got %d funds", limit, len(result.Funds))
		}
		if result.Showing != len(result.Funds) {
			t.Errorf("limit=%d: showing=%d but len(funds)=%d",
				limit, result.Showing, len(result.Funds))
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Cache behaviour
// ─────────────────────────────────────────────────────────────────────────────

func TestCache_HitHeaderPresent(t *testing.T) {
	srv := testServer(t)
	path := "/funds/rank?category=Equity&window=3Y"

	// First call warms the cache.
	r1, _ := http.Get(srv.URL + path)
	r1.Body.Close()
	if r1.Header.Get("X-Cache") != "MISS" {
		t.Errorf("first request: want X-Cache: MISS, got %q",
			r1.Header.Get("X-Cache"))
	}

	// Second call should hit.
	r2, _ := http.Get(srv.URL + path)
	r2.Body.Close()
	if r2.Header.Get("X-Cache") != "HIT" {
		t.Errorf("second request: want X-Cache: HIT, got %q",
			r2.Header.Get("X-Cache"))
	}
}

func TestCache_InvalidatedBySyncTrigger(t *testing.T) {
	srv := testServer(t)
	path := "/funds/rank?category=Equity&window=3Y"

	// Warm cache.
	r1, _ := http.Get(srv.URL + path)
	r1.Body.Close()

	// Trigger sync – should invalidate.
	tr, err := http.Post(srv.URL+"/sync/trigger", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /sync/trigger: %v", err)
	}
	tr.Body.Close()
	if tr.StatusCode != http.StatusAccepted {
		t.Errorf("trigger: want 202, got %d", tr.StatusCode)
	}

	// Next GET must be a MISS.
	r2, _ := http.Get(srv.URL + path)
	r2.Body.Close()
	if r2.Header.Get("X-Cache") == "HIT" {
		t.Error("expected cache MISS after sync trigger, got HIT")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmarks
// ─────────────────────────────────────────────────────────────────────────────

func BenchmarkRankFunds_CacheMiss(b *testing.B) {
	srv := testServer(b)
	client := srv.Client()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Unique limit per iteration → always a cache miss.
		url := fmt.Sprintf(
			"%s/funds/rank?category=Equity&window=3Y&limit=%d",
			srv.URL, 5+(i%100))
		resp, err := client.Get(url)
		if err != nil {
			b.Fatal(err)
		}
		resp.Body.Close()
	}
}

func BenchmarkRankFunds_CacheHit(b *testing.B) {
	srv := testServer(b)
	client := srv.Client()

	// Warm the cache once.
	r, _ := client.Get(srv.URL + "/funds/rank?category=Equity&window=3Y&limit=5")
	r.Body.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(srv.URL + "/funds/rank?category=Equity&window=3Y&limit=5")
		if err != nil {
			b.Fatal(err)
		}
		resp.Body.Close()
	}
}

func BenchmarkListFunds(b *testing.B) {
	srv := testServer(b)
	client := srv.Client()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(srv.URL + "/funds?category=Equity")
		if err != nil {
			b.Fatal(err)
		}
		resp.Body.Close()
	}
}

func BenchmarkGetAnalytics(b *testing.B) {
	srv := testServer(b)
	client := srv.Client()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(srv.URL + "/funds/119598/analytics?window=3Y")
		if err != nil {
			b.Fatal(err)
		}
		resp.Body.Close()
	}
}

