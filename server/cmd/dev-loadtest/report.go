package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/uptrace/bun"

	"github.com/Chukwuemekaeze/anibants-dot-federation/server/db/models"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/jobs"
)

// deliveryCounter records, per post, how many times a delivery job ran.
// A mutex guards the maps because up to WorkerConcurrency jobs run at once.
type deliveryCounter struct {
	mu        sync.Mutex
	delivered map[uuid.UUID]int // the post existed and was delivered
	missing   map[uuid.UUID]int // the job ran but the post did not exist
}

func newDeliveryCounter() *deliveryCounter {
	return &deliveryCounter{delivered: map[uuid.UUID]int{}, missing: map[uuid.UUID]int{}}
}

// wrap returns a handler that checks the post exists, runs the real handler,
// and counts the outcome.
func (c *deliveryCounter) wrap(database *bun.DB, real asynq.HandlerFunc) asynq.HandlerFunc {
	return func(ctx context.Context, task *asynq.Task) error {
		var p jobs.DeliverPostPayload
		if err := json.Unmarshal(task.Payload(), &p); err != nil {
			return err
		}
		exists, err := database.NewSelect().Model((*models.Post)(nil)).Where("id = ?", p.PostID).Exists(ctx)
		if err != nil {
			return err
		}
		if !exists {
			c.add(c.missing, p.PostID)
			return nil
		}
		if err := real(ctx, task); err != nil {
			return err
		}
		c.add(c.delivered, p.PostID)
		return nil
	}
}

func (c *deliveryCounter) add(m map[uuid.UUID]int, id uuid.UUID) {
	c.mu.Lock()
	m[id]++
	c.mu.Unlock()
}

type result struct {
	mode              string
	attempted         int
	failedOnPurpose   int
	relayCrashes      int
	relayKills        int
	duplicatesRefused int
	relayLog          string
	createdIn, total  time.Duration
}

func report(ctx context.Context, e env, c *deliveryCounter, r result) {
	var savedIDs []uuid.UUID
	if err := e.db.NewSelect().Model((*models.Post)(nil)).Column("id").Scan(ctx, &savedIDs); err != nil {
		fail("read posts", err)
	}
	saved := make(map[uuid.UUID]bool, len(savedIDs))
	for _, id := range savedIDs {
		saved[id] = true
	}

	var lost, once, twicePlus int
	for id := range saved {
		switch n := c.delivered[id]; {
		case n == 0:
			lost++
		case n == 1:
			once++
		default:
			twicePlus++
		}
	}
	// Jobs that ran for posts which, by the end, still do not exist.
	neverExisted := 0
	for id := range c.missing {
		if !saved[id] {
			neverExisted++
		}
	}

	fmt.Println()
	fmt.Printf("mode:                                %s\n", r.mode)
	fmt.Printf("posts attempted:                     %d\n", r.attempted)
	fmt.Printf("failed on purpose:                   %d\n", r.failedOnPurpose)
	fmt.Printf("posts saved:                         %d\n", len(saved))
	fmt.Printf("saved posts delivered 0 times:       %d   (lost)\n", lost)
	fmt.Printf("saved posts delivered exactly once:  %d\n", once)
	fmt.Printf("saved posts delivered 2+ times:      %d   (duplicates got through)\n", twicePlus)
	fmt.Printf("jobs for posts that never existed:   %d\n", neverExisted)
	if r.mode == "outbox" {
		fmt.Printf("relay crashes injected:              %d\n", r.relayCrashes)
		fmt.Printf("relays killed with SIGKILL:          %d\n", r.relayKills)
		fmt.Printf("duplicates refused by job queue:     %d\n", r.duplicatesRefused)
	}
	fmt.Printf("time to create posts:                %s\n", r.createdIn.Round(time.Millisecond))
	fmt.Printf("time until fully delivered:          %s\n", r.total.Round(time.Millisecond))

	if r.mode == "outbox" {
		pass := lost == 0 && twicePlus == 0 && neverExisted == 0 &&
			len(saved) == r.attempted-r.failedOnPurpose
		verdict := "PASS: every saved post delivered exactly once"
		if !pass {
			verdict = "FAIL: see the counts above"
		}
		fmt.Printf("\n%s\n(relay logs: %s)\n", verdict, r.relayLog)
	}
}
