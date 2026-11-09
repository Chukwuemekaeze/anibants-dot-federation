// Command job-q-worker runs background jobs from the job queue Redis, and the
// outbox relay that feeds jobs into it. Like running `celery worker` next to
// your FastAPI app.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/hibiken/asynq"

	"github.com/Chukwuemekaeze/anibants-dot-federation/server/config"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/db"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/jobs"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/logging"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/outbox"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("start worker", "err", err)
		os.Exit(1)
	}
	if err := logging.Setup(cfg.LogFormat, cfg.LogLevel); err != nil {
		slog.Error("start worker", "err", err)
		os.Exit(1)
	}

	// ctx is cancelled when you press Ctrl+C or the system sends a stop signal.
	// Everything started below watches it, so they all stop together.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Jobs read posts and the relay reads the outbox, so both need the database.
	database, err := db.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("start worker", "err", err)
		os.Exit(1)
	}
	defer database.Close()

	redisOpt := asynq.RedisClientOpt{Addr: cfg.RedisJobsAddr}

	// The relay: moves outbox rows into the job queue.
	queue := asynq.NewClient(redisOpt)
	defer queue.Close()
	relay := &outbox.Relay{
		DB:        database,
		Queue:     queue,
		Interval:  cfg.OutboxPollInterval,
		BatchSize: cfg.OutboxBatchSize,
	}

	// The worker: runs jobs from the job queue.
	srv := asynq.NewServer(redisOpt, asynq.Config{
		// How many jobs run at once, each in its own goroutine.
		Concurrency: cfg.WorkerConcurrency,
		// Send Asynq's own messages through slog so all output looks the same.
		Logger: asynqLogger{},
	})
	handlers := &jobs.Handlers{DB: database}
	// The mux routes each job type to its handler, like a router for jobs.
	mux := asynq.NewServeMux()
	mux.HandleFunc(jobs.TypeDeliverPost, handlers.DeliverPost)

	slog.Info("worker starting",
		"redis", cfg.RedisJobsAddr,
		"concurrency", cfg.WorkerConcurrency,
		"outbox_poll", cfg.OutboxPollInterval,
	)

	// Run the relay in its own goroutine. The WaitGroup lets main wait for it
	// to finish its current batch before exiting.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		relay.Run(ctx)
	}()

	// Start the worker without blocking, so main can watch for the stop signal.
	if err := srv.Start(mux); err != nil {
		slog.Error("start worker", "err", err)
		os.Exit(1)
	}

	<-ctx.Done() // wait here until Ctrl+C
	slog.Info("stopping: relay finishes its batch, worker finishes running jobs")
	srv.Shutdown()
	wg.Wait()
}

// asynqLogger adapts slog to the logger shape Asynq expects.
type asynqLogger struct{}

func (asynqLogger) Debug(args ...any) { slog.Debug(fmt.Sprint(args...), "source", "asynq") }
func (asynqLogger) Info(args ...any)  { slog.Info(fmt.Sprint(args...), "source", "asynq") }
func (asynqLogger) Warn(args ...any)  { slog.Warn(fmt.Sprint(args...), "source", "asynq") }
func (asynqLogger) Error(args ...any) { slog.Error(fmt.Sprint(args...), "source", "asynq") }
func (asynqLogger) Fatal(args ...any) {
	slog.Error(fmt.Sprint(args...), "source", "asynq")
	os.Exit(1)
}
