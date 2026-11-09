// Command job-q-worker runs background jobs from the job queue Redis.
// Like running `celery worker` next to your FastAPI app.
package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/hibiken/asynq"

	"github.com/Chukwuemekaeze/anibants-dot-federation/server/config"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/jobs"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/logging"
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

	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: cfg.RedisJobsAddr},
		asynq.Config{
			// How many jobs run at once, each in its own goroutine.
			Concurrency: cfg.WorkerConcurrency,
			// Send Asynq's own messages through slog so all output looks the same.
			Logger: asynqLogger{},
		},
	)

	// The mux routes each job type to its handler, like a router for jobs.
	mux := asynq.NewServeMux()
	mux.HandleFunc(jobs.TypeDeliverPost, jobs.HandleDeliverPost)

	slog.Info("worker starting", "redis", cfg.RedisJobsAddr, "concurrency", cfg.WorkerConcurrency)
	// Run blocks until Ctrl+C or a stop signal, then lets running jobs finish.
	if err := srv.Run(mux); err != nil {
		slog.Error("worker stopped", "err", err)
		os.Exit(1)
	}
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
