// Command dev-seed creates the fixed development test data: a user, an anime
// and a Circle. Safe to run more than once. A development tool only.
//
//	go run ./cmd/dev-seed
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/Chukwuemekaeze/anibants-dot-federation/server/config"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/db"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/devseed"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/logging"
)

func main() {
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

	if err := devseed.Seed(ctx, database); err != nil {
		slog.Error("seed", "err", err)
		os.Exit(1)
	}
	slog.Info("seeded", "user_id", devseed.UserID, "anime_id", devseed.AnimeID, "circle_id", devseed.CircleID)
}
