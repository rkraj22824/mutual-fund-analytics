# ── Stage 1: Build ─────────────────────────────────────────────────────────────
FROM golang:1.21-alpine AS builder

# No CGO needed (modernc.org/sqlite is pure Go)
ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64

WORKDIR /app

# Cache dependency downloads
COPY go.mod go.sum ./
RUN go mod download

# Build
COPY . .
RUN go build -ldflags="-s -w" -o /app/bin/mf-analytics ./cmd/server

# ── Stage 2: Runtime ───────────────────────────────────────────────────────────
FROM alpine:3.19

# ca-certificates for outbound HTTPS to api.mfapi.in
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /app/bin/mf-analytics .

# Data directory for SQLite file
RUN mkdir -p /app/data

EXPOSE 8080

ENV SERVER_PORT=8080 \
    DATABASE_PATH=/app/data/mf_analytics.db \
    MFAPI_BASE_URL=https://api.mfapi.in \
    RATE_PER_SECOND=2 \
    RATE_PER_MINUTE=50 \
    RATE_PER_HOUR=300 \
    SYNC_INTERVAL_HOURS=24 \
    BACKFILL_YEARS=10

ENTRYPOINT ["/app/mf-analytics"]

