package config

import (
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

// Each test runs in an empty temporary folder with the relevant variables
// cleared, so the real server/.env and your shell never leak in.
func isolate(t *testing.T) {
	t.Chdir(t.TempDir())
	for _, key := range []string{
		"DATABASE_URL", "REDIS_JOBS_ADDR", "REDIS_CACHE_ADDR",
		"LOG_FORMAT", "LOG_LEVEL", "WORKER_CONCURRENCY",
		"OUTBOX_POLL_INTERVAL", "OUTBOX_BATCH_SIZE",
	} {
		t.Setenv(key, "")
		os.Unsetenv(key)
	}
}

func TestMissingRequiredSettingFails(t *testing.T) {
	isolate(t)

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Fatalf("expected an error naming DATABASE_URL, got %v", err)
	}
}

func TestDefaultsApply(t *testing.T) {
	isolate(t)
	t.Setenv("DATABASE_URL", "postgres://example")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RedisJobsAddr != "localhost:6380" || cfg.RedisCacheAddr != "localhost:6381" {
		t.Fatalf("redis defaults not applied: %+v", cfg)
	}
	if cfg.LogFormat != "text" || cfg.LogLevel != slog.LevelInfo || cfg.WorkerConcurrency != 20 {
		t.Fatalf("logging or worker defaults not applied: %+v", cfg)
	}
	if cfg.OutboxPollInterval != 500*time.Millisecond || cfg.OutboxBatchSize != 100 {
		t.Fatalf("outbox defaults not applied: %+v", cfg)
	}
}

func TestMisspeltLogLevelFails(t *testing.T) {
	isolate(t)
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("LOG_LEVEL", "loud")

	if _, err := Load(); err == nil {
		t.Fatal("expected an error for an unknown log level")
	}
}

func TestReadsDotEnvButRealEnvironmentWins(t *testing.T) {
	isolate(t)
	dotenv := "DATABASE_URL=postgres://from-file\nREDIS_JOBS_ADDR=file-host:1\n"
	if err := os.WriteFile(".env", []byte(dotenv), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("REDIS_JOBS_ADDR", "real-host:2")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DatabaseURL != "postgres://from-file" {
		t.Fatalf(".env not read: %q", cfg.DatabaseURL)
	}
	if cfg.RedisJobsAddr != "real-host:2" {
		t.Fatalf("real environment should win over .env, got %q", cfg.RedisJobsAddr)
	}
}
