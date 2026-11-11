package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	defaultHTTPAddr = ":8080"
	maxRequestSize  = 1 << 20
	defaultPageSize = 20
	maxPageSize     = 100
)

type server struct {
	db *sql.DB
}

type job struct {
	ID              string          `json:"id"`
	IdempotencyKey  string          `json:"idempotency_key"`
	Status          string          `json:"status"`
	Payload         json.RawMessage `json:"payload"`
	WorkerID        *string         `json:"worker_id"`
	LeaseExpiresAt  *time.Time      `json:"lease_expires_at"`
	SideEffectCount int             `json:"side_effect_count"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
}

func main() {
	var err error
	switch {
	case len(os.Args) == 1:
		err = run()
	case len(os.Args) == 2 && os.Args[1] == "worker":
		err = runWorker()
	default:
		err = fmt.Errorf("usage: %s [worker]", os.Args[0])
	}
	if err != nil {
		log.Fatal(err)
	}
}

func run() error {
	db, err := openDatabase()
	if err != nil {
		return err
	}
	defer db.Close()

	count, err := configuredWorkerCount()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	workers, err := newWorkerPool(db, count)
	if err != nil {
		stop()
		return err
	}
	workers.start(ctx)
	defer func() {
		stop()
		workers.wait()
	}()

	srv := &http.Server{
		Addr:              envOrDefault("HTTP_ADDR", defaultHTTPAddr),
		Handler:           newRouter(db),
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("HTTP server listening on %s", srv.Addr)
		serverErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve HTTP: %w", err)
		}
		return nil
	case <-ctx.Done():
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shut down HTTP server: %w", err)
	}
	if err := <-serverErr; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve HTTP: %w", err)
	}
	return nil
}

func runWorker() error {
	db, err := openDatabase()
	if err != nil {
		return err
	}
	defer db.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	workers, err := newWorkerPool(db, 1)
	if err != nil {
		return err
	}
	workers.start(ctx)
	<-ctx.Done()
	workers.wait()
	return nil
}

func openDatabase() (*sql.DB, error) {
	db, err := sql.Open("pgx", databaseURL())
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL connection: %w", err)
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)

	pingCtx, cancelPing := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelPing()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	return db, nil
}

func configuredWorkerCount() (int, error) {
	raw := os.Getenv("WORKER_COUNT")
	if raw == "" {
		return workerCount, nil
	}
	count, err := strconv.Atoi(raw)
	if err != nil || count < 0 || count > 256 {
		return 0, fmt.Errorf("WORKER_COUNT must be an integer between 0 and 256")
	}
	return count, nil
}

func databaseURL() string {
	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		return dsn
	}

	query := url.Values{}
	query.Set("sslmode", envOrDefault("DB_SSLMODE", "disable"))
	return (&url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(envOrDefault("DB_USER", "soup"), envOrDefault("DB_PASSWORD", "soup_password")),
		Host:     net.JoinHostPort(envOrDefault("DB_HOST", "localhost"), envOrDefault("DB_PORT", "5432")),
		Path:     envOrDefault("DB_NAME", "job_queue"),
		RawQuery: query.Encode(),
	}).String()
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func newRouter(db *sql.DB) http.Handler {
	s := &server{db: db}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("POST /jobs", s.createJob)
	mux.HandleFunc("GET /jobs/{id}", s.getJob)
	mux.HandleFunc("GET /jobs", s.listJobs)
	mux.HandleFunc("POST /jobs/{id}/cancel", s.cancelJob)
	if os.Getenv("ENABLE_DB_STATS_ENDPOINT") == "true" {
		mux.HandleFunc("GET /debug/db-stats", func(w http.ResponseWriter, _ *http.Request) {
			stats := db.Stats()
			var databaseConnections, activeDatabaseConnections, databaseMaxConnections int
			err := db.QueryRow(`
				SELECT
					count(*) FILTER (WHERE datname = current_database()),
					count(*) FILTER (WHERE datname = current_database() AND state = 'active'),
					current_setting('max_connections')::integer
				FROM pg_stat_activity`).Scan(
				&databaseConnections,
				&activeDatabaseConnections,
				&databaseMaxConnections,
			)
			if err != nil {
				log.Printf("read database connection stats: %v", err)
				writeError(w, http.StatusInternalServerError, "could not read database stats")
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{
				"max_open_connections":        stats.MaxOpenConnections,
				"open_connections":            stats.OpenConnections,
				"in_use_connections":          stats.InUse,
				"idle_connections":            stats.Idle,
				"wait_count":                  stats.WaitCount,
				"wait_duration_ms":            float64(stats.WaitDuration) / float64(time.Millisecond),
				"max_idle_closed":             stats.MaxIdleClosed,
				"max_idle_time_closed":        stats.MaxIdleTimeClosed,
				"max_lifetime_closed":         stats.MaxLifetimeClosed,
				"database_connections":        databaseConnections,
				"database_active_connections": activeDatabaseConnections,
				"database_max_connections":    databaseMaxConnections,
			})
		})
	}
	return mux
}

func (s *server) createJob(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" {
		writeError(w, http.StatusBadRequest, "Idempotency-Key header is required")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxRequestSize)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request struct {
		Payload json.RawMessage `json:"payload"`
	}
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "request body must be valid JSON with a payload")
		return
	}
	if len(request.Payload) == 0 || !json.Valid(request.Payload) {
		writeError(w, http.StatusBadRequest, "payload must be valid JSON")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "request body must contain a single JSON value")
		return
	}

	const insertQuery = `
		INSERT INTO jobs (idempotency_key, payload)
		VALUES ($1, $2::jsonb)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id, idempotency_key, status, payload, worker_id, lease_expires_at,
		          side_effect_count, created_at, updated_at`
	created, err := scanJob(s.db.QueryRowContext(r.Context(), insertQuery, key, []byte(request.Payload)))
	if err == nil {
		writeJSON(w, http.StatusCreated, created)
		return
	}
	if !errors.Is(err, sql.ErrNoRows) {
		log.Printf("create job: %v", err)
		writeError(w, http.StatusInternalServerError, "could not create job")
		return
	}

	existing, err := s.findJobByIdempotencyKey(r.Context(), key)
	if err != nil {
		log.Printf("find idempotent job: %v", err)
		writeError(w, http.StatusInternalServerError, "could not retrieve existing job")
		return
	}
	writeJSON(w, http.StatusOK, existing)
}

func (s *server) getJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validUUID(id) {
		writeError(w, http.StatusBadRequest, "job id must be a UUID")
		return
	}
	found, err := s.findJobByID(r.Context(), id)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		log.Printf("get job %s: %v", id, err)
		writeError(w, http.StatusInternalServerError, "could not retrieve job")
		return
	}
	writeJSON(w, http.StatusOK, found)
}

func (s *server) listJobs(w http.ResponseWriter, r *http.Request) {
	limit := defaultPageSize
	if raw := r.URL.Query().Get("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > maxPageSize {
			writeError(w, http.StatusBadRequest, "limit must be between 1 and 100")
			return
		}
		limit = value
	}
	offset := 0
	if raw := r.URL.Query().Get("offset"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 0 {
			writeError(w, http.StatusBadRequest, "offset must be a non-negative integer")
			return
		}
		offset = value
	}

	rows, err := s.db.QueryContext(r.Context(), `
		SELECT id, idempotency_key, status, payload, worker_id, lease_expires_at,
		       side_effect_count, created_at, updated_at
		FROM jobs
		ORDER BY created_at DESC, id DESC
		LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		log.Printf("list jobs: %v", err)
		writeError(w, http.StatusInternalServerError, "could not list jobs")
		return
	}
	defer rows.Close()

	jobs := make([]job, 0)
	for rows.Next() {
		item, err := scanJob(rows)
		if err != nil {
			log.Printf("scan job: %v", err)
			writeError(w, http.StatusInternalServerError, "could not list jobs")
			return
		}
		jobs = append(jobs, item)
	}
	if err := rows.Err(); err != nil {
		log.Printf("iterate jobs: %v", err)
		writeError(w, http.StatusInternalServerError, "could not list jobs")
		return
	}
	writeJSON(w, http.StatusOK, jobs)
}

func (s *server) cancelJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validUUID(id) {
		writeError(w, http.StatusBadRequest, "job id must be a UUID")
		return
	}

	const cancelQuery = `
		UPDATE jobs
		SET status = 'cancelled', updated_at = now()
		WHERE id = $1 AND status = 'queued'
		RETURNING id, idempotency_key, status, payload, worker_id, lease_expires_at,
		          side_effect_count, created_at, updated_at`
	cancelled, err := scanJob(s.db.QueryRowContext(r.Context(), cancelQuery, id))
	if err == nil {
		writeJSON(w, http.StatusOK, cancelled)
		return
	}
	if !errors.Is(err, sql.ErrNoRows) {
		log.Printf("cancel job %s: %v", id, err)
		writeError(w, http.StatusInternalServerError, "could not cancel job")
		return
	}

	current, err := s.findJobByID(r.Context(), id)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		log.Printf("check job %s after cancellation: %v", id, err)
		writeError(w, http.StatusInternalServerError, "could not cancel job")
		return
	}
	writeJSON(w, http.StatusConflict, map[string]string{
		"error":  "only queued jobs can be cancelled",
		"status": current.Status,
	})
}

func (s *server) findJobByID(ctx context.Context, id string) (job, error) {
	return scanJob(s.db.QueryRowContext(ctx, `
		SELECT id, idempotency_key, status, payload, worker_id, lease_expires_at,
		       side_effect_count, created_at, updated_at
		FROM jobs WHERE id = $1`, id))
}

func (s *server) findJobByIdempotencyKey(ctx context.Context, key string) (job, error) {
	return scanJob(s.db.QueryRowContext(ctx, `
		SELECT id, idempotency_key, status, payload, worker_id, lease_expires_at,
		       side_effect_count, created_at, updated_at
		FROM jobs WHERE idempotency_key = $1`, key))
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanJob(row rowScanner) (job, error) {
	var item job
	var payload []byte
	err := row.Scan(
		&item.ID,
		&item.IdempotencyKey,
		&item.Status,
		&payload,
		&item.WorkerID,
		&item.LeaseExpiresAt,
		&item.SideEffectCount,
		&item.CreatedAt,
		&item.UpdatedAt,
	)
	if err != nil {
		return job{}, err
	}
	item.Payload = json.RawMessage(payload)
	return item, nil
}

func validUUID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for i, char := range value {
		switch i {
		case 8, 13, 18, 23:
			if char != '-' {
				return false
			}
		default:
			if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f') || (char >= 'A' && char <= 'F')) {
				return false
			}
		}
	}
	return true
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write JSON response: %v", err)
	}
}
