package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

const (
	workerCount        = 4
	workerPollInterval = 500 * time.Millisecond
	workerLease        = 30 * time.Second
	recoveryInterval   = 5 * time.Second
	recoveryBatchSize  = 100
)

var errLeaseLost = errors.New("job lease expired or was reassigned")

type workerPool struct {
	db        *sql.DB
	workerIDs []string
	waitGroup sync.WaitGroup
}

type claimedJob struct {
	id       string
	workerID string
}

func newWorkerPool(db *sql.DB, count int) (*workerPool, error) {
	host, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("get hostname for worker IDs: %w", err)
	}

	pool := &workerPool{db: db, workerIDs: make([]string, count)}
	for i := range pool.workerIDs {
		pool.workerIDs[i] = fmt.Sprintf("%s-%d-%d", host, os.Getpid(), i+1)
	}
	return pool, nil
}

func (p *workerPool) start(ctx context.Context) {
	if len(p.workerIDs) == 0 {
		log.Printf("started with background workers disabled")
		return
	}
	for _, workerID := range p.workerIDs {
		p.waitGroup.Add(1)
		go func() {
			defer p.waitGroup.Done()
			p.run(ctx, workerID)
		}()
	}
	p.waitGroup.Add(1)
	go func() {
		defer p.waitGroup.Done()
		p.recoverExpiredJobs(ctx)
	}()
	log.Printf("started %d job workers and the lease recovery cleaner", len(p.workerIDs))
}

func (p *workerPool) wait() {
	p.waitGroup.Wait()
}

func (p *workerPool) run(ctx context.Context, workerID string) {
	for ctx.Err() == nil {
		claimed, ok, err := p.claim(ctx, workerID)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("worker %s could not claim a job: %v", workerID, err)
			if !waitForNextPoll(ctx) {
				return
			}
			continue
		}
		if !ok {
			if !waitForNextPoll(ctx) {
				return
			}
			continue
		}
		p.process(ctx, claimed)
	}
}

func (p *workerPool) claim(ctx context.Context, workerID string) (claimedJob, bool, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return claimedJob{}, false, err
	}
	defer tx.Rollback()

	var id string
	err = tx.QueryRowContext(ctx, `
		SELECT id
		FROM jobs
		WHERE status = 'queued'
		ORDER BY created_at, id
		LIMIT 1
		FOR UPDATE SKIP LOCKED`).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return claimedJob{}, false, nil
	}
	if err != nil {
		return claimedJob{}, false, fmt.Errorf("select job to claim: %w", err)
	}

	var claimedID string
	err = tx.QueryRowContext(ctx, `
		UPDATE jobs
		SET status = 'running',
		    worker_id = $2,
		    lease_expires_at = now() + $3 * interval '1 second',
		    updated_at = now()
		WHERE id = $1 AND status = 'queued'
		RETURNING id`, id, workerID, int64(workerLease.Seconds())).Scan(&claimedID)
	if err != nil {
		return claimedJob{}, false, fmt.Errorf("update claimed job: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return claimedJob{}, false, fmt.Errorf("commit job claim: %w", err)
	}

	return claimedJob{id: claimedID, workerID: workerID}, true, nil
}

func (p *workerPool) recoverExpiredJobs(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		for {
			recovered, err := p.recoverExpiredBatch(ctx)
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("lease recovery failed: %v", err)
				}
				break
			}
			if recovered > 0 {
				log.Printf("lease recovery requeued %d expired jobs", recovered)
			}
			if recovered < recoveryBatchSize {
				break
			}
		}

		timer.Reset(recoveryInterval)
	}
}

func (p *workerPool) recoverExpiredBatch(ctx context.Context) (int, error) {
	rows, err := p.db.QueryContext(ctx, `
		WITH expired_jobs AS (
			SELECT id
			FROM jobs
			WHERE status = 'running' AND lease_expires_at < now()
			ORDER BY lease_expires_at, id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs AS job
		SET status = 'queued',
		    worker_id = NULL,
		    lease_expires_at = NULL,
		    updated_at = now()
		FROM expired_jobs
		WHERE job.id = expired_jobs.id
		  AND job.status = 'running'
		  AND job.lease_expires_at < now()
		RETURNING job.id`, recoveryBatchSize)
	if err != nil {
		return 0, fmt.Errorf("requeue expired jobs: %w", err)
	}
	defer rows.Close()

	recovered := 0
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return 0, fmt.Errorf("scan recovered job ID: %w", err)
		}
		recovered++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate recovered jobs: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close recovered jobs result: %w", err)
	}
	return recovered, nil
}

func (p *workerPool) process(ctx context.Context, claimed claimedJob) {
	if err := p.complete(ctx, claimed); err != nil {
		if errors.Is(err, errLeaseLost) {
			log.Printf("worker %s lost lease for job %s", claimed.workerID, claimed.id)
			return
		}
		if ctx.Err() != nil {
			return
		}
		log.Printf("worker %s failed job %s: %v", claimed.workerID, claimed.id, err)
		failed, failErr := p.fail(ctx, claimed)
		if failErr != nil {
			log.Printf("worker %s could not mark job %s failed: %v", claimed.workerID, claimed.id, failErr)
		} else if !failed {
			log.Printf("worker %s could not mark job %s failed because its lease ended", claimed.workerID, claimed.id)
		}
		return
	}
	log.Printf("worker %s completed job %s", claimed.workerID, claimed.id)
}

func (p *workerPool) complete(ctx context.Context, claimed claimedJob) error {
	result, err := p.db.ExecContext(ctx, `
		UPDATE jobs
		SET side_effect_count = side_effect_count + 1,
		    status = 'succeeded',
		    worker_id = NULL,
		    lease_expires_at = NULL,
		    updated_at = now()
		WHERE id = $1
		  AND status = 'running'
		  AND worker_id = $2
		  AND lease_expires_at > now()`, claimed.id, claimed.workerID)
	if err != nil {
		return fmt.Errorf("record execution side effect and completion: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check job completion result: %w", err)
	}
	if rows == 0 {
		return errLeaseLost
	}
	return nil
}

func (p *workerPool) fail(ctx context.Context, claimed claimedJob) (bool, error) {
	result, err := p.db.ExecContext(ctx, `
		UPDATE jobs
		SET status = 'failed',
		    worker_id = NULL,
		    lease_expires_at = NULL,
		    updated_at = now()
		WHERE id = $1
		  AND status = 'running'
		  AND worker_id = $2
		  AND lease_expires_at > now()`, claimed.id, claimed.workerID)
	if err != nil {
		return false, fmt.Errorf("update job state to failed: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("check failed job update result: %w", err)
	}
	return rows > 0, nil
}

func waitForNextPoll(ctx context.Context) bool {
	timer := time.NewTimer(workerPollInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
