// Package outbox implements the transactional outbox. Add writes a job into
// the outbox table inside the caller's transaction, and the Relay moves those
// rows into the Asynq job queue. Every saved change is eventually queued;
// a crash can at worst cause a duplicate, which Asynq refuses by job ID.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/uptrace/bun"

	"github.com/Chukwuemekaeze/anibants-dot-federation/server/db/models"
)

// How long Asynq keeps a finished job. While it is kept, queuing another job
// with the same ID is refused, which catches duplicates from relay crashes.
const dedupRetention = 24 * time.Hour

// Enqueuer is anything that can queue a job, like a Python Protocol.
// The real Asynq client qualifies automatically because it has this method.
type Enqueuer interface {
	EnqueueContext(ctx context.Context, task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error)
}

// Fails the build if the real Asynq client ever stops matching Enqueuer.
var _ Enqueuer = (*asynq.Client)(nil)

// Add writes a job into the outbox. Pass the transaction that saves the
// change, so the job and the change are committed together or not at all.
// bun.IDB accepts either a transaction or the plain database.
func Add(ctx context.Context, db bun.IDB, task *asynq.Task) error {
	id, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("make outbox id: %w", err)
	}
	msg := &models.OutboxMessage{ID: id, JobType: task.Type(), Payload: task.Payload()}
	if _, err := db.NewInsert().Model(msg).Exec(ctx); err != nil {
		return fmt.Errorf("add %s job to outbox: %w", task.Type(), err)
	}
	return nil
}

// Relay polls the outbox and moves rows into the job queue.
type Relay struct {
	DB        *bun.DB
	Queue     Enqueuer
	Interval  time.Duration
	BatchSize int
}

// Run polls until ctx is cancelled. When a batch comes back full, it goes
// again straight away instead of waiting, so a backlog drains quickly.
func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		for {
			sent, err := r.RelayBatch(ctx)
			if err != nil && ctx.Err() == nil {
				slog.Error("outbox relay", "err", err)
			}
			if sent > 0 {
				slog.Info("outbox relayed", "jobs", sent)
			}
			if err != nil || sent < r.BatchSize {
				break
			}
		}
	}
}

// RelayBatch moves up to BatchSize rows into the job queue and returns how
// many it moved. Rows it could not queue stay in the outbox for next time.
func (r *Relay) RelayBatch(ctx context.Context) (int, error) {
	var sent int
	var queueErr error

	txErr := r.DB.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		// Oldest first. FOR UPDATE locks the rows we take until we commit;
		// SKIP LOCKED makes a second relay skip them instead of waiting,
		// so two relays never take the same row.
		var msgs []models.OutboxMessage
		err := tx.NewSelect().Model(&msgs).
			Order("id").
			Limit(int64(r.BatchSize)). // Bun wants an int64; Go never converts number types implicitly

			For("UPDATE SKIP LOCKED").
			Scan(ctx)
		if err != nil {
			return fmt.Errorf("read outbox: %w", err)
		}

		var done []uuid.UUID
		for _, m := range msgs {
			task := asynq.NewTask(m.JobType, m.Payload)
			_, err := r.Queue.EnqueueContext(ctx, task,
				asynq.TaskID(m.ID.String()),     // the outbox row's ID becomes the job ID
				asynq.Retention(dedupRetention), // keep it after it finishes, to refuse duplicates
			)
			switch {
			case err == nil:
				done = append(done, m.ID)
			case errors.Is(err, asynq.ErrTaskIDConflict):
				// Already queued by an earlier attempt: a duplicate, correctly refused.
				slog.Warn("outbox duplicate refused by job queue", "outbox_id", m.ID, "job_type", m.JobType)
				done = append(done, m.ID)
			default:
				// Probably Redis is down. Keep the rest for the next poll.
				queueErr = fmt.Errorf("queue outbox job %s: %w", m.ID, err)
			}
			if queueErr != nil {
				break
			}
		}

		if len(done) > 0 {
			_, err := tx.NewDelete().Model((*models.OutboxMessage)(nil)).
				Where("id IN (?)", bun.In(done)).
				Exec(ctx)
			if err != nil {
				return fmt.Errorf("delete relayed outbox rows: %w", err)
			}
		}
		sent = len(done)
		return nil
	})

	if txErr != nil {
		return 0, txErr
	}
	return sent, queueErr
}
