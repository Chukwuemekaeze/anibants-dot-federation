// Package logging sets up slog once at program start, like configuring
// Python's root logger in main. After Setup, any code can call slog.Info(...).
package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/lmittmann/tint"
	slogmulti "github.com/samber/slog-multi"
	slogsampling "github.com/samber/slog-sampling"
)

// Setup makes the configured logger the default for the whole program.
func Setup(format string, level slog.Level) error {
	handler, err := newHandler(os.Stderr, format, level)
	if err != nil {
		return err
	}
	slog.SetDefault(slog.New(handler))
	return nil
}

func newHandler(w io.Writer, format string, level slog.Level) (slog.Handler, error) {
	var base slog.Handler
	switch format {
	case "text":
		// Readable, coloured lines for your terminal.
		base = tint.NewHandler(w, &tint.Options{Level: level})
	case "json":
		// One JSON object per line, which log tools read.
		base = slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})
	default:
		return nil, fmt.Errorf("unknown LOG_FORMAT %q: use text or json", format)
	}

	// Noise control: lines with the same level and message are grouped. Each
	// second, the first 100 of a group are kept and 1% of the rest. The next
	// kept line carries dropped_count, so a flood is still visible.
	sampling := slogsampling.ThresholdSamplingOption{
		Tick:                time.Second,
		Threshold:           100,
		Rate:                0.01,
		IncludeDroppedCount: true,
	}

	return slogmulti.Pipe(sampling.NewMiddleware()).Handler(base), nil
}
