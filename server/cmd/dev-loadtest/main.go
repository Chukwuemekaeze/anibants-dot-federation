// Command dev-loadtest compares post delivery with and without the
// transactional outbox under load and crashes. It creates many posts
// concurrently, injects failures, waits until everything has drained, then
// reports how many times each saved post was delivered.
//
// Modes:
//
//	-mode naive-save-first   save, crash at rate -fail, then queue
//	-mode naive-queue-first  queue, crash at rate -fail, then save
//	-mode outbox             the real posts.Create plus separate relay
//	                         processes, with transactions failing just before
//	                         commit (-fail), relays crashing after queuing but
//	                         before deleting (-relay-fail), and one relay
//	                         killed with SIGKILL after -kill-after
//
// It counts everything in the database and job queue, so run it on fresh,
// throwaway Postgres and Redis with no worker running. Development only.
//
//	go run ./cmd/dev-loadtest -mode outbox -n 10000
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/uptrace/bun"

	"github.com/Chukwuemekaeze/anibants-dot-federation/server/config"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/db"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/db/models"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/devseed"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/jobs"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/logging"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/posts"
)

type options struct {
	mode          string
	n             int
	concurrency   int
	failRate      float64
	relays        int
	relayFailRate float64
	killAfter     time.Duration
	timeout       time.Duration
}

// env holds the connections every part of the load test shares.
type env struct {
	cfg       config.Config
	db        *bun.DB
	redisOpt  asynq.RedisClientOpt
	rdb       *redis.Client
	inspector *asynq.Inspector
}

func main() {
	// The same program doubles as a relay process, so the load test can
	// start real relay processes and kill one of them.
	if len(os.Args) > 1 && os.Args[1] == "relay" {
		runRelayProcess(os.Args[2:])
		return
	}
	runLoadTest()
}

func runLoadTest() {
	var o options
	flag.StringVar(&o.mode, "mode", "outbox", "naive-save-first, naive-queue-first or outbox")
	flag.IntVar(&o.n, "n", 10000, "number of posts to attempt")
	flag.IntVar(&o.concurrency, "concurrency", 50, "posts created at the same time")
	flag.Float64Var(&o.failRate, "fail", 0.1, "chance a post creation fails between its two writes")
	flag.IntVar(&o.relays, "relays", 3, "relay processes (outbox mode)")
	flag.Float64Var(&o.relayFailRate, "relay-fail", 0.01, "chance a relay crashes after queuing a job, before deleting its row (outbox mode)")
	flag.DurationVar(&o.killAfter, "kill-after", time.Second, "SIGKILL one relay after this long, 0 to skip (outbox mode)")
	flag.DurationVar(&o.timeout, "timeout", 5*time.Minute, "give up waiting for everything to drain after this long")
	flag.Parse()

	switch o.mode {
	case "naive-save-first", "naive-queue-first", "outbox":
	default:
		fail("start", fmt.Errorf("unknown -mode %q", o.mode))
	}

	ctx := context.Background()
	e := setup(ctx)
	defer e.db.Close()
	defer e.rdb.Close()
	defer e.inspector.Close()

	if err := devseed.Seed(ctx, e.db); err != nil {
		fail("seed", err)
	}
	ensureFresh(ctx, e)

	// The job runner, inside this process, with a wrapper that counts deliveries.
	counter := newDeliveryCounter()
	srv := startJobRunner(e, counter)

	var sup *relaySupervisor
	if o.mode == "outbox" {
		sup = startRelays(o)
		if o.killAfter > 0 {
			go func() {
				time.Sleep(o.killAfter)
				sup.killAndRestart(0)
			}()
		}
	}

	start := time.Now()
	attempted, failed := createPosts(ctx, e, o)
	createdIn := time.Since(start)

	waitForDrain(ctx, e, o, sup)
	total := time.Since(start)

	srv.Shutdown()
	r := result{
		mode: o.mode, attempted: attempted, failedOnPurpose: failed,
		createdIn: createdIn, total: total,
	}
	if sup != nil {
		sup.stopAll()
		r.relayKills = sup.kills
		r.relayCrashes, _ = e.rdb.Get(ctx, keyRelayCrashes).Int()
		r.duplicatesRefused, _ = e.rdb.Get(ctx, keyDuplicatesRefused).Int()
		r.relayLog = sup.logPath
	}
	report(ctx, e, counter, r)
}

func setup(ctx context.Context) env {
	cfg, err := config.Load()
	if err != nil {
		fail("start", err)
	}
	// Warnings and errors only: 10,000 "delivered" lines would bury the report.
	if err := logging.Setup(cfg.LogFormat, slog.LevelWarn); err != nil {
		fail("start", err)
	}
	database, err := db.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		fail("start", err)
	}
	redisOpt := asynq.RedisClientOpt{Addr: cfg.RedisJobsAddr}
	return env{
		cfg:       cfg,
		db:        database,
		redisOpt:  redisOpt,
		rdb:       redis.NewClient(&redis.Options{Addr: cfg.RedisJobsAddr}),
		inspector: asynq.NewInspector(redisOpt),
	}
}

// ensureFresh refuses to run unless the database and job queue are empty,
// because the report counts everything in them.
func ensureFresh(ctx context.Context, e env) {
	if servers, err := e.inspector.Servers(); err == nil && len(servers) > 0 {
		fail("start", errors.New("a worker is running on this job queue: stop it first"))
	}
	postCount, err := e.db.NewSelect().Model((*models.Post)(nil)).WhereAllWithDeleted().Count(ctx)
	if err != nil {
		fail("start", err)
	}
	outboxCount, err := e.db.NewSelect().Model((*models.OutboxMessage)(nil)).Count(ctx)
	if err != nil {
		fail("start", err)
	}
	if postCount > 0 || outboxCount > 0 || queueBusy(e) > 0 {
		fail("start", errors.New("posts, outbox or job queue is not empty: run on fresh throwaway databases"))
	}
	e.rdb.Del(ctx, keyRelayCrashes, keyDuplicatesRefused)
}

// createPosts creates o.n posts from o.concurrency goroutines at once and
// returns how many were attempted and how many failed on purpose.
func createPosts(ctx context.Context, e env, o options) (attempted, failedOnPurpose int) {
	if o.mode == "outbox" {
		// Lets the tool fail chosen transactions just before they commit.
		e.db.AddQueryHook(commitFaultHook{})
	}

	var failed atomic.Int64
	work := make(chan int)
	var wg sync.WaitGroup

	for w := 0; w < o.concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				in := posts.CreateInput{
					CircleID: devseed.CircleID,
					AuthorID: devseed.UserID,
					Body:     fmt.Sprintf("load test post %d", i),
				}

				var err error
				switch o.mode {
				case "naive-save-first":
					err = createSaveFirst(ctx, e.db, e.redisOpt, in, o.failRate)
				case "naive-queue-first":
					err = createQueueFirst(ctx, e.db, e.redisOpt, in, o.failRate)
				case "outbox":
					callCtx, release, doomed := withCommitFault(ctx, o.failRate)
					_, err = posts.Create(callCtx, e.db, in)
					release()
					if doomed {
						if err == nil {
							fail("create post", errors.New("a transaction marked to fail committed anyway"))
						}
						err = errSimulatedCrash
					}
				}

				if errors.Is(err, errSimulatedCrash) {
					failed.Add(1)
				} else if err != nil {
					fail("create post", err)
				}
			}
		}()
	}

	for i := 1; i <= o.n; i++ {
		work <- i
	}
	close(work)
	wg.Wait()
	return o.n, int(failed.Load())
}

// startJobRunner runs the real delivery handler inside this process,
// wrapped so every delivery is counted per post.
func startJobRunner(e env, counter *deliveryCounter) *asynq.Server {
	srv := asynq.NewServer(e.redisOpt, asynq.Config{
		Concurrency: e.cfg.WorkerConcurrency,
		LogLevel:    asynq.ErrorLevel,
	})
	handlers := &jobs.Handlers{DB: e.db}
	mux := asynq.NewServeMux()
	mux.HandleFunc(jobs.TypeDeliverPost, counter.wrap(e.db, handlers.DeliverPost))
	if err := srv.Start(mux); err != nil {
		fail("start job runner", err)
	}
	return srv
}

// waitForDrain waits until the outbox is empty and no jobs are left in the
// job queue, a few checks in a row, and any scheduled relay kill has happened.
func waitForDrain(ctx context.Context, e env, o options, sup *relaySupervisor) {
	deadline := time.Now().Add(o.timeout)
	idleChecks := 0
	lastProgress := time.Now()

	for time.Now().Before(deadline) {
		time.Sleep(250 * time.Millisecond)

		outboxCount, err := e.db.NewSelect().Model((*models.OutboxMessage)(nil)).Count(ctx)
		if err != nil {
			fail("check outbox", err)
		}
		busy := queueBusy(e)

		if time.Since(lastProgress) > 2*time.Second {
			fmt.Fprintf(os.Stderr, "  draining: %d outbox rows, %d jobs in the job queue\n", outboxCount, busy)
			lastProgress = time.Now()
		}

		killPending := sup != nil && o.killAfter > 0 && !sup.killedOnce()
		if outboxCount == 0 && busy == 0 && !killPending {
			idleChecks++
			if idleChecks >= 4 {
				return
			}
		} else {
			idleChecks = 0
		}
	}
	fail("drain", fmt.Errorf("still not drained after %s", o.timeout))
}

// queueBusy is how many jobs are waiting, running or due to retry.
func queueBusy(e env) int {
	// GetQueueInfo reports a missing queue with an internal error type that
	// outside code cannot check for, so ask which queues exist first.
	queues, err := e.inspector.Queues()
	if err != nil {
		fail("read job queue", err)
	}
	if !slices.Contains(queues, "default") {
		return 0 // nothing has ever been queued
	}
	info, err := e.inspector.GetQueueInfo("default")
	if err != nil {
		fail("read job queue", err)
	}
	return info.Pending + info.Active + info.Scheduled + info.Retry
}

func fail(msg string, err error) {
	slog.Error(msg, "err", err)
	os.Exit(1)
}
