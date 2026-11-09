package logging

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestUnknownFormatFails(t *testing.T) {
	if _, err := newHandler(&bytes.Buffer{}, "xml", slog.LevelInfo); err == nil {
		t.Fatal("expected an error for an unknown format")
	}
}

func TestLevelFiltersLowerLines(t *testing.T) {
	var out bytes.Buffer
	h, err := newHandler(&out, "json", slog.LevelWarn)
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(h)

	logger.Info("hidden")
	logger.Warn("shown")

	if strings.Contains(out.String(), "hidden") || !strings.Contains(out.String(), "shown") {
		t.Fatalf("level filter not applied:\n%s", out.String())
	}
}

func TestSamplingThinsRepeatedLines(t *testing.T) {
	var out bytes.Buffer
	h, err := newHandler(&out, "json", slog.LevelInfo)
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(h)

	// 1,000 identical lines within one second.
	for i := 0; i < 1000; i++ {
		logger.Info("same message", "i", i)
	}

	kept := strings.Count(out.String(), "same message")
	// The first 100 always pass, then about 1% of the remaining 900.
	if kept < 100 || kept > 130 {
		t.Fatalf("expected about 109 lines kept, got %d", kept)
	}
}
