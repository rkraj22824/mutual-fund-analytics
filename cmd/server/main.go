package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mf-analytics/internal/analytics"
	"mf-analytics/internal/api"
	"mf-analytics/internal/config"
	"mf-analytics/internal/database"
	"mf-analytics/internal/ingestion"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("[boot] starting MF Analytics Service")

	cfg := config.Load()

	// ── Database ─────────────────────────────────────────────────────────────
	db, err := database.New(cfg.DatabasePath)
	if err != nil {
		log.Fatalf("[boot] database init: %v", err)
	}
	defer db.Close()

	if err := db.RunMigrations(); err != nil {
		log.Fatalf("[boot] migrations: %v", err)
	}
	log.Println("[boot] database ready")

	// ── Rate limiter (restores sliding-window state from DB) ──────────────────
	rateLimiter := ingestion.NewRateLimiter(cfg, db)
	log.Printf("[boot] rate limiter ready (%d/s  %d/min  %d/hr)",
		cfg.RatePerSecond, cfg.RatePerMinute, cfg.RatePerHour)

	// ── mfapi.in HTTP client ──────────────────────────────────────────────────
	mfClient := ingestion.NewClient(cfg, rateLimiter)

	// ── Analytics engine ──────────────────────────────────────────────────────
	analyticsEngine := analytics.NewEngine(db)

	// ── Data pipeline ────────────────────────────────────────────────────────
	pipeline := ingestion.NewPipeline(cfg, db, mfClient, analyticsEngine)

	// ── HTTP router ───────────────────────────────────────────────────────────
	router := api.NewRouter(db, pipeline, analyticsEngine)

	// ── Background pipeline ───────────────────────────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pipeline.Run(ctx)
	log.Println("[boot] pipeline started")

	// ── HTTP server ───────────────────────────────────────────────────────────
	srv := &http.Server{
		Addr:         ":" + cfg.ServerPort,
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("[boot] HTTP server listening on :%s", cfg.ServerPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[http] server error: %v", err)
		}
	}()

	<-quit
	log.Println("[shutdown] signal received, draining…")

	cancel() // signal pipeline to stop

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("[shutdown] http server error: %v", err)
	}
	log.Println("[shutdown] service stopped cleanly")
}


