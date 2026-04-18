package ingestion

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"mf-analytics/internal/database"
	"mf-analytics/internal/models"
)

// targetSchemes is the authoritative list of 10 funds we track.
// Codes verified against mfapi.in  (curl "https://api.mfapi.in/mf/search?q=...")
var targetSchemes = []models.Scheme{
	// ── ICICI Prudential ──────────────────────────────────────────────────────
	{
		Code: "120505", AMC: "ICICI Prudential Mutual Fund",
		Category: models.CategoryMidCap,
		Name:     "ICICI Prudential Midcap Fund - Direct Plan - Growth",
	},
	{
		Code: "120841", AMC: "ICICI Prudential Mutual Fund",
		Category: models.CategorySmallCap,
		Name:     "ICICI Prudential Smallcap Fund - Direct Plan - Growth",
	},

	// ── HDFC ─────────────────────────────────────────────────────────────────
	{
		Code: "118989", AMC: "HDFC Mutual Fund",
		Category: models.CategoryMidCap,
		Name:     "HDFC Mid-Cap Opportunities Fund - Direct Plan - Growth",
	},
	{
		Code: "125354", AMC: "HDFC Mutual Fund",
		Category: models.CategorySmallCap,
		Name:     "HDFC Small Cap Fund - Direct Plan - Growth",
	},

	// ── Axis ─────────────────────────────────────────────────────────────────
	{
		Code: "119598", AMC: "Axis Mutual Fund",
		Category: models.CategoryMidCap,
		Name:     "Axis Midcap Fund - Direct Plan - Growth",
	},
	{
		Code: "125494", AMC: "Axis Mutual Fund",
		Category: models.CategorySmallCap,
		Name:     "Axis Small Cap Fund - Direct Plan - Growth",
	},

	// ── SBI ───────────────────────────────────────────────────────────────────
	{
		Code: "119775", AMC: "SBI Mutual Fund",
		Category: models.CategoryMidCap,
		Name:     "SBI Magnum Midcap Fund - Direct Plan - Growth",
	},
	{
		Code: "125349", AMC: "SBI Mutual Fund",
		Category: models.CategorySmallCap,
		Name:     "SBI Small Cap Fund - Direct Plan - Growth",
	},

	// ── Kotak Mahindra ────────────────────────────────────────────────────────
	{
		Code: "119007", AMC: "Kotak Mahindra Mutual Fund",
		Category: models.CategoryMidCap,
		Name:     "Kotak Emerging Equity Fund - Direct Plan - Growth",
	},
	{
		Code: "120594", AMC: "Kotak Mahindra Mutual Fund",
		Category: models.CategorySmallCap,
		Name:     "Kotak Small Cap Fund - Direct Plan - Growth",
	},
}

// TargetSchemes returns a copy of the authoritative scheme list.
func TargetSchemes() []models.Scheme { return targetSchemes }

// SeedSchemes upserts each target scheme.
// Uses ON CONFLICT … DO UPDATE so that category/name corrections in the
// targetSchemes slice propagate to an already-populated database (e.g. after
// a category rename like "Mid Cap" → "Equity: Mid Cap").
func SeedSchemes(db *database.DB) error {
	for _, s := range targetSchemes {
		_, err := db.Exec(`
			INSERT INTO schemes (code, name, amc, category)
			VALUES (?, ?, ?, ?)
			ON CONFLICT(code) DO UPDATE SET
			    name     = excluded.name,
			    amc      = excluded.amc,
			    category = excluded.category`,
			s.Code, s.Name, s.AMC, s.Category,
		)
		if err != nil {
			return fmt.Errorf("seed scheme %s: %w", s.Code, err)
		}
	}
	log.Printf("[schemes] seeded/updated %d target schemes", len(targetSchemes))
	return nil
}

// DiscoverAndVerify uses the mfapi search endpoint to confirm/correct scheme
// codes against live API data.
func DiscoverAndVerify(client *Client, db *database.DB) error {
	log.Println("[discovery] verifying scheme codes against mfapi.in …")

	for _, target := range targetSchemes {
		query := buildSearchQuery(target.Name)
		results, err := client.SearchSchemes(query)
		if err != nil {
			log.Printf("[discovery] search failed for %q: %v (skipping)", query, err)
			continue
		}

		best := bestMatch(results, target)
		if best == nil {
			log.Printf("[discovery] no match found for %s (%s)", target.Code, target.Name)
			continue
		}

		if best.Code != target.Code {
			log.Printf("[discovery] code update: %s → %s for %q",
				target.Code, best.Code, target.Name)
			if err := migrateSchemeCode(db, target.Code, best.Code, best.Name); err != nil {
				log.Printf("[discovery] migrate code: %v", err)
			}
		} else {
			log.Printf("[discovery] ✓ %s  %s", target.Code, target.Name)
		}
	}
	return nil
}

func buildSearchQuery(name string) string {
	name = strings.ReplaceAll(name, " - Direct Plan - Growth", "")
	name = strings.ReplaceAll(name, " - Direct Plan", "")
	name = strings.ReplaceAll(name, " - Growth", "")
	return strings.TrimSpace(name)
}

func bestMatch(results []SchemeSearchResult, target models.Scheme) *SchemeSearchResult {
	keywords := strings.Fields(strings.ToLower(target.Name))
	bestScore := 0
	var best *SchemeSearchResult

	for i := range results {
		r := &results[i]
		rLower := strings.ToLower(r.Name)
		if !strings.Contains(rLower, "direct") || !strings.Contains(rLower, "growth") {
			continue
		}
		score := 0
		for _, kw := range keywords {
			if strings.Contains(rLower, kw) {
				score++
			}
		}
		if score > bestScore {
			bestScore = score
			best = r
		}
	}
	return best
}

func migrateSchemeCode(db *database.DB, oldCode, newCode, newName string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var exists int
	tx.QueryRow("SELECT COUNT(*) FROM schemes WHERE code = ?", newCode).Scan(&exists)
	if exists == 0 {
		_, err = tx.Exec(`UPDATE schemes SET code = ?, name = ? WHERE code = ?`,
			newCode, newName, oldCode)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// LoadSchemesFromDB returns tracked schemes, optionally filtered.
// category and amc are matched with LIKE so partial values work
// (e.g. category="Equity" matches "Equity: Mid Cap" and "Equity: Small Cap").
func LoadSchemesFromDB(db *database.DB, category, amc string) ([]models.Scheme, error) {
	query := `SELECT code, name, amc, category, last_synced_at, created_at
	          FROM schemes WHERE 1=1`
	args := []any{}

	if category != "" {
		query += " AND LOWER(category) LIKE '%' || LOWER(?) || '%'"
		args = append(args, category)
	}
	if amc != "" {
		query += " AND LOWER(amc) LIKE '%' || LOWER(?) || '%'"
		args = append(args, amc)
	}
	query += " ORDER BY amc, category, name"

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	schemes := make([]models.Scheme, 0) // never nil → JSON []
	for rows.Next() {
		var s models.Scheme
		var lastSyncRaw sql.NullString
		var createdRaw string
		if err := rows.Scan(
			&s.Code, &s.Name, &s.AMC, &s.Category,
			&lastSyncRaw, &createdRaw,
		); err != nil {
			return nil, err
		}
		if lastSyncRaw.Valid {
			t, _ := time.Parse(time.RFC3339, lastSyncRaw.String)
			s.LastSyncAt = &t
		}
		s.CreatedAt, _ = time.Parse(time.RFC3339, createdRaw)
		schemes = append(schemes, s)
	}
	return schemes, rows.Err()
}

