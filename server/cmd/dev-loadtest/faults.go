package main

import (
	"context"
	"errors"
	"math/rand/v2"
	"strings"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/uptrace/bun"

	"github.com/Chukwuemekaeze/anibants-dot-federation/server/outbox"
)

// Dev-only counters kept in the job queue Redis, because the relays run as
// separate processes and logs are sampled.
const (
	keyRelayCrashes      = "dev-loadtest:relay_crashes"
	keyDuplicatesRefused = "dev-loadtest:duplicates_refused"
)

// ---- Failing a transaction just before it commits (outbox mode) ----

// commitFaultKey marks a call whose transaction should fail before commit.
type commitFaultKey struct{}

// withCommitFault decides at random whether this call's transaction fails.
// If so, the returned context carries its own cancel function, which the
// query hook below calls right after the outbox insert. Cancelling a
// transaction's context makes Postgres roll it back, so the post and its
// outbox row both disappear, exactly like a crash just before COMMIT.
func withCommitFault(ctx context.Context, failRate float64) (context.Context, context.CancelFunc, bool) {
	if rand.Float64() >= failRate {
		return ctx, func() {}, false
	}
	ctx, cancel := context.WithCancel(ctx)
	return context.WithValue(ctx, commitFaultKey{}, cancel), cancel, true
}

// commitFaultHook is a Bun query hook: Bun calls it around every query.
// It leaves posts.Create untouched and fails transactions from the outside.
type commitFaultHook struct{}

func (commitFaultHook) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context {
	return ctx
}

func (commitFaultHook) AfterQuery(ctx context.Context, event *bun.QueryEvent) {
	cancel, marked := ctx.Value(commitFaultKey{}).(context.CancelFunc)
	if marked && strings.HasPrefix(event.Query, `INSERT INTO "outbox"`) {
		cancel() // both inserts are done; the commit is now doomed
	}
}

// ---- Relays crashing after queuing but before deleting (outbox mode) ----

var errInjectedRelayCrash = errors.New("injected relay crash after queuing")

// faultyRelayQueue wraps the real Asynq client for the relay processes.
// After a job is queued successfully it sometimes reports failure anyway,
// so the relay leaves the row in the outbox, as if it crashed before the
// delete. It also counts duplicates the job queue refuses.
type faultyRelayQueue struct {
	real     outbox.Enqueuer
	failRate float64
	rdb      *redis.Client
}

// Fails the build if faultyRelayQueue ever stops counting as an Enqueuer.
var _ outbox.Enqueuer = faultyRelayQueue{}

func (q faultyRelayQueue) EnqueueContext(ctx context.Context, task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	info, err := q.real.EnqueueContext(ctx, task, opts...)
	if errors.Is(err, asynq.ErrTaskIDConflict) {
		q.rdb.Incr(context.Background(), keyDuplicatesRefused)
		return info, err
	}
	if err == nil && rand.Float64() < q.failRate {
		q.rdb.Incr(context.Background(), keyRelayCrashes)
		return nil, errInjectedRelayCrash
	}
	return info, err
}
