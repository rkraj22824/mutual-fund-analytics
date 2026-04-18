package api

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"

	"mf-analytics/internal/analytics"
	"mf-analytics/internal/database"
	"mf-analytics/internal/models"
)

// PipelineSyncer is the minimal interface the HTTP layer needs from the
// ingestion pipeline. Exported so test packages can supply a lightweight stub.
// *ingestion.Pipeline satisfies this interface automatically.
type PipelineSyncer interface {
	TriggerSync(ctx context.Context) error
	Status() (models.PipelineStatus, error)
}

// NewRouter wires all routes, middleware, and the response cache.
func NewRouter(
	db *database.DB,
	pipeline PipelineSyncer,
	analyticsEngine *analytics.Engine,
) http.Handler {

	cache := NewCache()

	h := &Handlers{
		db:        db,
		pipeline:  pipeline,
		analytics: analyticsEngine,
		cache:     cache,
	}

	r := chi.NewRouter()

	r.Use(recoveryMiddleware)
	r.Use(loggingMiddleware)
	r.Use(corsMiddleware)

	r.Get("/health", h.handleHealth)

	// /funds/rank MUST be declared before /funds/{code} so chi routes the
	// literal string "rank" before treating it as a URL parameter.
	r.Route("/funds", func(r chi.Router) {
		r.Get("/", h.handleListFunds)
		r.Get("/rank", h.handleRankFunds)
		r.Get("/{code}", h.handleGetFund)
		r.Get("/{code}/analytics", h.handleGetAnalytics)
	})

	r.Route("/sync", func(r chi.Router) {
		r.Post("/trigger", h.handleSyncTrigger)
		r.Get("/status", h.handleSyncStatus)
	})

	return r
}

