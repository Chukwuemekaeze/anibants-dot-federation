package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"

	"github.com/Chukwuemekaeze/anibants-dot-federation/server/config"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/db"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/logging"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/outbox"
)

// runRelayProcess is what a relay process runs: the real outbox relay,
// handed a job queue that sometimes "crashes" after queuing.
func runRelayProcess(args []string) {
	fs := flag.NewFlagSet("relay", flag.ExitOnError)
	failRate := fs.Float64("relay-fail", 0, "chance of a crash after queuing")
	fs.Parse(args)

	cfg, err := config.Load()
	if err != nil {
		fail("relay start", err)
	}
	if err := logging.Setup(cfg.LogFormat, cfg.LogLevel); err != nil {
		fail("relay start", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	database, err := db.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		fail("relay start", err)
	}
	defer database.Close()

	client := asynq.NewClient(asynq.RedisClientOpt{Addr: cfg.RedisJobsAddr})
	defer client.Close()
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisJobsAddr})
	defer rdb.Close()

	relay := &outbox.Relay{
		DB:        database,
		Queue:     faultyRelayQueue{real: client, failRate: *failRate, rdb: rdb},
		Interval:  cfg.OutboxPollInterval,
		BatchSize: cfg.OutboxBatchSize,
	}
	relay.Run(ctx)
}

// relaySupervisor starts the relay processes and can kill one with SIGKILL,
// which is what `kill -9` sends: the process dies instantly, mid-batch,
// with no chance to clean up.
type relaySupervisor struct {
	mu       sync.Mutex
	procs    []*exec.Cmd
	args     []string
	logFile  *os.File
	logPath  string
	kills    int
	killDone bool
}

func startRelays(o options) *relaySupervisor {
	logPath := filepath.Join(os.TempDir(), "dev-loadtest-relays.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		fail("start relays", err)
	}
	s := &relaySupervisor{
		procs:   make([]*exec.Cmd, o.relays),
		args:    []string{"relay", "-relay-fail", fmt.Sprint(o.relayFailRate)},
		logFile: logFile,
		logPath: logPath,
	}
	for i := range s.procs {
		s.start(i)
	}
	return s
}

func (s *relaySupervisor) start(i int) {
	self, err := os.Executable()
	if err != nil {
		fail("start relay", err)
	}
	cmd := exec.Command(self, s.args...)
	cmd.Env = os.Environ()
	cmd.Stdout = s.logFile
	cmd.Stderr = s.logFile
	if err := cmd.Start(); err != nil {
		fail("start relay", err)
	}
	s.procs[i] = cmd
}

// killAndRestart kills relay i with SIGKILL, then starts a replacement.
func (s *relaySupervisor) killAndRestart(i int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.procs[i].Process.Kill() // SIGKILL
	s.procs[i].Wait()
	s.kills++
	s.killDone = true
	s.start(i)
}

func (s *relaySupervisor) killedOnce() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.killDone
}

// stopAll asks every relay to stop cleanly and waits for them.
func (s *relaySupervisor) stopAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.procs {
		p.Process.Signal(syscall.SIGTERM)
		p.Wait()
	}
	s.logFile.Close()
}
