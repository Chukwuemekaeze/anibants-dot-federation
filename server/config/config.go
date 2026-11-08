// Package config loads settings from the environment, like pydantic-settings.
// Load it once in main and pass the result to whatever needs it.
package config

import (
	"errors"
	"fmt"
	"io/fs"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

// Config lists every setting the programs read. Each tag names the
// environment variable it comes from.
type Config struct {
	DatabaseURL    string `env:"DATABASE_URL,required"`
	RedisJobsAddr  string `env:"REDIS_JOBS_ADDR" envDefault:"localhost:6380"`
	RedisCacheAddr string `env:"REDIS_CACHE_ADDR" envDefault:"localhost:6381"`
}

// Load reads .env from the current folder if there is one, then fills Config
// from the environment. Real environment variables always win over .env,
// so a server can set them directly and needs no .env file.
func Load() (Config, error) {
	if err := godotenv.Load(); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return Config{}, fmt.Errorf("read .env: %w", err)
	}

	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	return cfg, nil
}
