package ingestion

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"mf-analytics/internal/config"
	"mf-analytics/internal/models"
)

const (
	maxRetries    = 3
	retryBaseWait = 2 * time.Second
	httpTimeout   = 30 * time.Second
	blockDuration = 5 * time.Minute
)

// ── mfapi.in response DTOs ────────────────────────────────────────────────────

type schemeDetailResponse struct {
	Meta struct {
		FundHouse      string `json:"fund_house"`
		SchemeType     string `json:"scheme_type"`
		SchemeCategory string `json:"scheme_category"`
		SchemeCode     int    `json:"scheme_code"`
		SchemeName     string `json:"scheme_name"`
	} `json:"meta"`
	Data []struct {
		Date string `json:"date"` // DD-MM-YYYY
		NAV  string `json:"nav"`
	} `json:"data"`
	Status string `json:"status"`
}

// SchemeSearchResult is one item from the /mf/search endpoint.
type SchemeSearchResult struct {
	Code string `json:"schemeCode"`
	Name string `json:"schemeName"`
}

func (s *SchemeSearchResult) UnmarshalJSON(b []byte) error {
	var raw struct {
		Code json.RawMessage `json:"schemeCode"`
		Name string          `json:"schemeName"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	s.Name = raw.Name
	s.Code = strings.Trim(string(raw.Code), `"`)
	return nil
}

// Client is the rate-limited HTTP client for mfapi.in.
type Client struct {
	baseURL     string
	http        *http.Client
	rateLimiter *RateLimiter
}

// NewClient constructs a Client.
func NewClient(cfg *config.Config, rl *RateLimiter) *Client {
	return &Client{
		baseURL:     cfg.MFAPIBaseURL,
		http:        &http.Client{Timeout: httpTimeout},
		rateLimiter: rl,
	}
}

// FetchSchemeHistory fetches the complete NAV history for a scheme code.
func (c *Client) FetchSchemeHistory(ctx context.Context, code string) ([]models.NAVPoint, *models.Scheme, error) {
	endpoint := fmt.Sprintf("%s/mf/%s", c.baseURL, code)
	body, err := c.doGet(ctx, endpoint)
	if err != nil {
		return nil, nil, fmt.Errorf("fetch history %s: %w", code, err)
	}

	var resp schemeDetailResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, nil, fmt.Errorf("parse history %s: %w", code, err)
	}
	if strings.ToUpper(resp.Status) != "SUCCESS" && resp.Status != "" {
		return nil, nil, fmt.Errorf("api status=%q for scheme %s", resp.Status, code)
	}

	scheme := &models.Scheme{
		Code:     strconv.Itoa(resp.Meta.SchemeCode),
		Name:     resp.Meta.SchemeName,
		AMC:      resp.Meta.FundHouse,
		Category: normalisedCategory(resp.Meta.SchemeCategory),
	}
	if scheme.Code == "0" || scheme.Code == "" {
		scheme.Code = code
	}

	var points []models.NAVPoint
	for _, d := range resp.Data {
		t, err := time.Parse("02-01-2006", d.Date)
		if err != nil {
			continue
		}
		nav, err := strconv.ParseFloat(strings.TrimSpace(d.NAV), 64)
		if err != nil || nav <= 0 {
			continue
		}
		points = append(points, models.NAVPoint{
			SchemeCode: code,
			Date:       t,
			NAV:        nav,
		})
	}
	return points, scheme, nil
}

// FetchLatestNAV fetches only the most recent NAV entry for a scheme.
func (c *Client) FetchLatestNAV(ctx context.Context, code string) (*models.NAVPoint, error) {
	endpoint := fmt.Sprintf("%s/mf/%s/latest", c.baseURL, code)
	body, err := c.doGet(ctx, endpoint)
	if err != nil {
		return nil, fmt.Errorf("fetch latest %s: %w", code, err)
	}

	var resp schemeDetailResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parse latest %s: %w", code, err)
	}
	if len(resp.Data) == 0 {
		return nil, fmt.Errorf("no data in latest response for %s", code)
	}

	d := resp.Data[0]
	t, err := time.Parse("02-01-2006", d.Date)
	if err != nil {
		return nil, fmt.Errorf("parse date %q: %w", d.Date, err)
	}
	nav, err := strconv.ParseFloat(strings.TrimSpace(d.NAV), 64)
	if err != nil {
		return nil, fmt.Errorf("parse nav %q: %w", d.NAV, err)
	}
	return &models.NAVPoint{SchemeCode: code, Date: t, NAV: nav}, nil
}

// SearchSchemes calls the mfapi search endpoint.
func (c *Client) SearchSchemes(query string) ([]SchemeSearchResult, error) {
	endpoint := fmt.Sprintf("%s/mf/search?q=%s", c.baseURL, url.QueryEscape(query))
	body, err := c.doGet(context.Background(), endpoint)
	if err != nil {
		return nil, err
	}
	var results []SchemeSearchResult
	if err := json.Unmarshal(body, &results); err != nil {
		return nil, err
	}
	return results, nil
}

// ── Internal HTTP machinery ───────────────────────────────────────────────────

func (c *Client) doGet(ctx context.Context, endpoint string) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := c.rateLimiter.Wait(ctx); err != nil {
			return nil, err
		}

		body, retry, err := c.tryGet(ctx, endpoint)
		if err == nil {
			return body, nil
		}
		lastErr = err
		if !retry {
			return nil, err
		}

		wait := retryBaseWait * time.Duration(1<<attempt)
		log.Printf("[client] retry %d/%d for %s in %s: %v",
			attempt+1, maxRetries, endpoint, wait, err)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}
	return nil, fmt.Errorf("all %d retries exhausted: %w", maxRetries, lastErr)
}

func (c *Client) tryGet(ctx context.Context, endpoint string) ([]byte, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "mf-analytics/1.0")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, true, fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
		if err != nil {
			return nil, true, fmt.Errorf("read body: %w", err)
		}
		return body, false, nil

	case http.StatusTooManyRequests:
		c.rateLimiter.SetBlocked(time.Now().Add(blockDuration))
		log.Printf("[client] 429 received – blocked for %s", blockDuration)
		return nil, true, fmt.Errorf("429 rate limited")

	case http.StatusNotFound:
		return nil, false, fmt.Errorf("404 not found: %s", endpoint)

	case http.StatusInternalServerError, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return nil, true, fmt.Errorf("server error %d", resp.StatusCode)

	default:
		return nil, false, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
}

// normalisedCategory maps mfapi.in scheme_category strings to the canonical
// category names used throughout this service.
// Examples of raw values: "Equity Scheme - Mid Cap Fund",
// "Equity Scheme - Small Cap Fund", "Mid Cap", "Small Cap".
func normalisedCategory(raw string) string {
	lower := strings.ToLower(raw)
	switch {
	case strings.Contains(lower, "mid cap") || strings.Contains(lower, "midcap"):
		return models.CategoryMidCap // "Equity: Mid Cap"
	case strings.Contains(lower, "small cap") || strings.Contains(lower, "smallcap"):
		return models.CategorySmallCap // "Equity: Small Cap"
	default:
		return raw
	}
}

