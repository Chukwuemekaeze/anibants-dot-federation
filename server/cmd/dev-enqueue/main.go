// Command dev-enqueue queues test "deliver post" jobs with made-up post IDs,
// so you can watch the worker pick them up. A development tool only.
//
//	go run ./cmd/dev-enqueue -n 5
package main

import (
	"flag"
	"log/slog"
	"os"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"

	"github.com/Chukwuemekaeze/anibants-dot-federation/server/config"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/jobs"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/logging"
)

func main() {
	n := flag.Int("n", 1, "number of test jobs to queue")
	flag.Parse()

	cfg, err := config.Load()
	if err != nil {
		slog.Error("start", "err", err)
		os.Exit(1)
	}
	if err := logging.Setup(cfg.LogFormat, cfg.LogLevel); err != nil {
		slog.Error("start", "err", err)
		os.Exit(1)
	}

	// The client side of Asynq: it only writes jobs into Redis.
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: cfg.RedisJobsAddr})
	defer client.Close()

	for i := 0; i < *n; i++ {
		postID, err := uuid.NewV7()
		if err != nil {
			slog.Error("make post id", "err", err)
			os.Exit(1)
		}

		task, err := jobs.NewDeliverPostTask(postID)
		if err != nil {
			slog.Error("build job", "err", err)
			os.Exit(1)
		}

		info, err := client.Enqueue(task)
		if err != nil {
			slog.Error("queue job", "err", err)
			os.Exit(1)
		}
		slog.Info("queued", "job_id", info.ID, "post_id", postID)
	}
}
