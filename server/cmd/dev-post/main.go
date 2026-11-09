// Command dev-post creates test posts in the seeded Circle through the real
// post creation path, which saves each post and its delivery job in the
// outbox together. The worker's outbox relay then queues the jobs.
// Run dev-seed first. A development tool only.
//
//	go run ./cmd/dev-post -n 5
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/Chukwuemekaeze/anibants-dot-federation/server/config"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/db"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/devseed"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/logging"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/posts"
)

func main() {
	n := flag.Int("n", 1, "number of test posts to create")
	flag.Parse()
	ctx := context.Background()

	cfg, err := config.Load()
	if err != nil {
		slog.Error("start", "err", err)
		os.Exit(1)
	}
	if err := logging.Setup(cfg.LogFormat, cfg.LogLevel); err != nil {
		slog.Error("start", "err", err)
		os.Exit(1)
	}

	database, err := db.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("start", "err", err)
		os.Exit(1)
	}
	defer database.Close()

	for i := 1; i <= *n; i++ {
		post, err := posts.Create(ctx, database, posts.CreateInput{
			CircleID: devseed.CircleID,
			AuthorID: devseed.UserID,
			Body:     fmt.Sprintf("test post %d", i),
		})
		if err != nil {
			slog.Error("create post (did you run dev-seed?)", "err", err)
			os.Exit(1)
		}
		slog.Info("created post", "post_id", post.ID, "body", post.Body)
	}
}
