package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

var integrationSequence atomic.Uint64

func integrationDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("set DATABASE_URL to a disposable database with schema.sql applied")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open integration database: %v", err)
	}
	db.SetMaxOpenConns(16)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		t.Fatalf("connect to integration database: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close integration database: %v", err)
		}
	})
	return db
}

func insertIntegrationJob(t *testing.T, db *sql.DB, status, workerID string) string {
	t.Helper()
	key := fmt.Sprintf("integration-%d-%d", time.Now().UnixNano(), integrationSequence.Add(1))
	var id string
	err := db.QueryRow(`
		INSERT INTO jobs (idempotency_key, status, payload, worker_id)
		VALUES ($1, $2, '{"integration":true}'::jsonb, NULLIF($3, ''))
		RETURNING id`, key, status, workerID).Scan(&id)
	if err != nil {
		t.Fatalf("insert integration job: %v", err)
	}
	t.Cleanup(func() {
		if _, err := db.Exec(`DELETE FROM jobs WHERE id = $1`, id); err != nil {
			t.Errorf("delete integration job %s: %v", id, err)
		}
	})
	return id
}

func TestIntegrationSkipLockedPreventsDoubleClaim(t *testing.T) {
	db := integrationDB(t)
	id := insertIntegrationJob(t, db, "queued", "")

	lockTx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lockTx.Rollback()
	var lockedID string
	if err := lockTx.QueryRow(`
		SELECT id FROM jobs WHERE id = $1 AND status = 'queued'
		FOR UPDATE`, id).Scan(&lockedID); err != nil {
		t.Fatalf("lock queued job: %v", err)
	}

	pool, err := newWorkerPool(db, 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	claimed, ok, err := pool.claim(ctx, "integration-lock-check")
	elapsed := time.Since(started)
	if err != nil {
		t.Fatalf("claim around locked row: %v", err)
	}
	if ok {
		t.Fatalf("worker claimed locked job %s", claimed.id)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("claim blocked for %s instead of skipping locked row", elapsed)
	}
	t.Logf("PASS double claim: locked queued job skipped in %s", elapsed)
}

func TestIntegrationZombieWriteRejectedAndLeaseRecovered(t *testing.T) {
	db := integrationDB(t)
	id := insertIntegrationJob(t, db, "running", "dead-worker")
	if _, err := db.Exec(`
		UPDATE jobs SET lease_expires_at = now() - interval '1 second'
		WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}

	pool, err := newWorkerPool(db, 0)
	if err != nil {
		t.Fatal(err)
	}
	zombie := claimedJob{id: id, workerID: "dead-worker"}
	if err := pool.complete(context.Background(), zombie); err != errLeaseLost {
		t.Fatalf("late completion error = %v, want %v", err, errLeaseLost)
	}

	started := time.Now()
	recovered, err := pool.recoverExpiredBatch(context.Background())
	recoveryDuration := time.Since(started)
	if err != nil {
		t.Fatalf("recover expired lease: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("recovered jobs = %d, want 1", recovered)
	}
	if err := pool.complete(context.Background(), zombie); err != errLeaseLost {
		t.Fatalf("zombie write after requeue error = %v, want %v", err, errLeaseLost)
	}

	var status string
	var workerID, lease sql.NullString
	if err := db.QueryRow(`
		SELECT status, worker_id, lease_expires_at::text FROM jobs WHERE id = $1`, id).
		Scan(&status, &workerID, &lease); err != nil {
		t.Fatal(err)
	}
	if status != "queued" || workerID.Valid || lease.Valid {
		t.Fatalf("recovered state = status:%s worker:%v lease:%v", status, workerID, lease)
	}
	t.Logf("PASS zombie protection and lease recovery: recovery query took %s", recoveryDuration)
}

func TestIntegrationCancelCompleteRace(t *testing.T) {
	db := integrationDB(t)
	id := insertIntegrationJob(t, db, "queued", "")
	router := newRouter(db)
	pool, err := newWorkerPool(db, 1)
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	cancelResponse := make(chan *httptest.ResponseRecorder, 1)
	claimResult := make(chan struct {
		job claimedJob
		ok  bool
		err error
	}, 1)
	var waitGroup sync.WaitGroup
	waitGroup.Add(2)
	go func() {
		defer waitGroup.Done()
		<-start
		request := httptest.NewRequest(http.MethodPost, "/jobs/"+id+"/cancel", nil)
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		cancelResponse <- response
	}()
	go func() {
		defer waitGroup.Done()
		<-start
		job, ok, err := pool.claim(context.Background(), "integration-race-worker")
		claimResult <- struct {
			job claimedJob
			ok  bool
			err error
		}{job: job, ok: ok, err: err}
	}()
	close(start)
	waitGroup.Wait()

	claim := <-claimResult
	if claim.err != nil {
		t.Fatalf("claim during cancel race: %v", claim.err)
	}
	if claim.ok {
		if err := pool.complete(context.Background(), claim.job); err != nil {
			t.Fatalf("complete raced job: %v", err)
		}
	}
	response := <-cancelResponse
	if response.Code != http.StatusOK && response.Code != http.StatusConflict {
		t.Fatalf("cancel returned HTTP %d: %s", response.Code, response.Body.String())
	}

	var status string
	var effects int
	if err := db.QueryRow(`SELECT status, side_effect_count FROM jobs WHERE id = $1`, id).
		Scan(&status, &effects); err != nil {
		t.Fatal(err)
	}
	if !((status == "cancelled" && effects == 0) || (status == "succeeded" && effects == 1)) {
		t.Fatalf("invalid cancel/complete race result: status=%s side_effect_count=%d", status, effects)
	}
	t.Logf("PASS cancel/complete race: HTTP %d, final status %s, side effects %d", response.Code, status, effects)
}

func TestIntegrationConcurrentIdempotencyRetries(t *testing.T) {
	db := integrationDB(t)
	router := newRouter(db)
	key := fmt.Sprintf("integration-duplicate-%d", time.Now().UnixNano())
	const attempts = 32
	type response struct {
		status int
		id     string
		err    error
	}
	responses := make(chan response, attempts)
	var waitGroup sync.WaitGroup
	var start sync.WaitGroup
	start.Add(1)
	for range attempts {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			start.Wait()
			request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(
				`{"payload":{"task":"idempotency-test"}}`))
			request.Header.Set("Idempotency-Key", key)
			request.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			var body struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
				responses <- response{status: recorder.Code, err: err}
				return
			}
			responses <- response{status: recorder.Code, id: body.ID}
		}()
	}
	start.Done()
	waitGroup.Wait()
	close(responses)

	created := 0
	jobIDs := make(map[string]bool)
	for result := range responses {
		if result.err != nil {
			t.Fatalf("decode duplicate response: %v", result.err)
		}
		switch result.status {
		case http.StatusCreated:
			created++
		case http.StatusOK:
		default:
			t.Fatalf("duplicate POST returned HTTP %d", result.status)
		}
		jobIDs[result.id] = true
	}
	if created != 1 || len(jobIDs) != 1 {
		t.Fatalf("created responses=%d distinct job IDs=%d, want 1 each", created, len(jobIDs))
	}
	t.Cleanup(func() {
		if _, err := db.Exec(`DELETE FROM jobs WHERE idempotency_key = $1`, key); err != nil {
			t.Errorf("delete duplicate test job: %v", err)
		}
	})
	t.Logf("PASS duplicate request retries: %d simultaneous requests produced one job", attempts)
}

func TestIntegrationRepeatableReadPreventsPhantom(t *testing.T) {
	db := integrationDB(t)
	prefix := fmt.Sprintf("integration-phantom-%d-", time.Now().UnixNano())
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	var before int
	if err := tx.QueryRow(`SELECT count(*) FROM jobs WHERE idempotency_key LIKE $1`, prefix+"%").
		Scan(&before); err != nil {
		t.Fatal(err)
	}
	key := prefix + "inserted"
	if _, err := db.Exec(`
		INSERT INTO jobs (idempotency_key, payload) VALUES ($1, '{"integration":true}'::jsonb)`, key); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := db.Exec(`DELETE FROM jobs WHERE idempotency_key = $1`, key); err != nil {
			t.Errorf("delete phantom test job: %v", err)
		}
	})

	var after int
	if err := tx.QueryRow(`SELECT count(*) FROM jobs WHERE idempotency_key LIKE $1`, prefix+"%").
		Scan(&after); err != nil {
		t.Fatal(err)
	}
	var outside int
	if err := db.QueryRow(`SELECT count(*) FROM jobs WHERE idempotency_key LIKE $1`, prefix+"%").
		Scan(&outside); err != nil {
		t.Fatal(err)
	}
	if after != before || outside != before+1 {
		t.Fatalf("repeatable-read counts before=%d inside-after=%d outside-after=%d", before, after, outside)
	}
	t.Logf("PASS transaction isolation: REPEATABLE READ held count at %d while a concurrent insert was visible outside", before)
}

func TestIntegrationLeaseUsesDatabaseClock(t *testing.T) {
	db := integrationDB(t)
	id := insertIntegrationJob(t, db, "queued", "")
	pool, err := newWorkerPool(db, 1)
	if err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := pool.claim(context.Background(), "integration-clock-worker")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || claimed.id != id {
		t.Fatalf("claimed job = %+v, ok=%t; want %s", claimed, ok, id)
	}

	var leaseInRange bool
	if err := db.QueryRow(`
		SELECT lease_expires_at > now() + interval '29 seconds'
		   AND lease_expires_at <= now() + interval '30 seconds'
		FROM jobs WHERE id = $1`, id).Scan(&leaseInRange); err != nil {
		t.Fatal(err)
	}
	if !leaseInRange {
		t.Fatal("lease expiry was not 29-30 seconds ahead of PostgreSQL NOW()")
	}
	t.Log("PASS clock skew: claimed lease is based on PostgreSQL NOW(), not client time")
}
