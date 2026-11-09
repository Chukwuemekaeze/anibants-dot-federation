// Command dev-gap shows the gap between saving a post and queuing its job.
// It creates posts with a simulated crash between the two steps, then
// compares the posts in Postgres with the jobs waiting in Redis.
//
// Run it on a fresh database and job queue with no worker running, because
// the report counts everything there. Run dev-seed first. Development only.
//
//	go run ./cmd/dev-gap -n 1000 -fail 0.1 -order save-first
//	go run ./cmd/dev-gap -n 1000 -fail 0.1 -order queue-first
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/uptrace/bun"

	"github.com/Chukwuemekaeze/anibants-dot-federation/server/config"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/db"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/db/models"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/devseed"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/jobs"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/logging"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/posts"
)

// errSimulatedCrash stands in for the program dying between the two steps.
var errSimulatedCrash = errors.New("simulated crash between the two steps")

func main() {
	n := flag.Int("n", 1000, "number of posts to attempt")
	failRate := flag.Float64("fail", 0.1, "chance of a crash between the two steps, 0 to 1")
	order := flag.String("order", "save-first", "save-first (the real posts.Create) or queue-first (reversed, demo only)")
	flag.Parse()
	ctx := context.Background()

	cfg, err := config.Load()
	if err != nil {
		fail("start", err)
	}
	if err := logging.Setup(cfg.LogFormat, cfg.LogLevel); err != nil {
		fail("start", err)
	}
	database, err := db.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		fail("start", err)
	}
	defer database.Close()

	redisOpt := asynq.RedisClientOpt{Addr: cfg.RedisJobsAddr}
	inspector := asynq.NewInspector(redisOpt)
	defer inspector.Close()
	// A running worker would take jobs out of Redis and spoil the count.
	if servers, err := inspector.Servers(); err == nil && len(servers) > 0 {
		fail("start", errors.New("a worker is running on this job queue: stop it first"))
	}

	client := asynq.NewClient(redisOpt)
	defer client.Close()

	in := posts.CreateInput{CircleID: devseed.CircleID, AuthorID: devseed.UserID}
	crashes := 0
	for i := 1; i <= *n; i++ {
		in.Body = fmt.Sprintf("gap test post %d", i)

		switch *order {
		case "save-first":
			// The real code path, handed a queue that sometimes fails.
			_, err = posts.Create(ctx, database, flakyQueue{real: client, failRate: *failRate}, in)
		case "queue-first":
			err = createQueueFirst(ctx, database, client, in, *failRate)
		default:
			fail("start", fmt.Errorf("unknown -order %q", *order))
		}

		if errors.Is(err, errSimulatedCrash) {
			crashes++
		} else if err != nil {
			fail("create post", err)
		}
	}

	report(ctx, database, inspector, *order, *n, crashes)
}

// flakyQueue wraps the real queue and fails a share of the calls. It has the
// same method as the real client, so it also counts as a posts.Enqueuer.
type flakyQueue struct {
	real     posts.Enqueuer
	failRate float64
}

func (q flakyQueue) EnqueueContext(ctx context.Context, task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	if rand.Float64() < q.failRate {
		return nil, errSimulatedCrash
	}
	return q.real.EnqueueContext(ctx, task, opts...)
}

// createQueueFirst is posts.Create with the two steps swapped. It exists only
// here, to show that changing the order moves the gap instead of closing it.
func createQueueFirst(ctx context.Context, database *bun.DB, queue posts.Enqueuer, in posts.CreateInput, failRate float64) error {
	id, err := uuid.NewV7()
	if err != nil {
		return err
	}

	// Step 1: queue the job.
	task, err := jobs.NewDeliverPostTask(id)
	if err != nil {
		return err
	}
	if _, err := queue.EnqueueContext(ctx, task); err != nil {
		return err
	}

	// The crash lands after the job is queued but before the post is saved.
	if rand.Float64() < failRate {
		return errSimulatedCrash
	}

	// Step 2: save the post.
	post := &models.Post{ID: id, CircleID: in.CircleID, AuthorID: in.AuthorID, Body: in.Body}
	_, err = database.NewInsert().Model(post).Exec(ctx)
	return err
}

// report compares the post IDs in Postgres with the post IDs inside the jobs
// waiting in Redis.
func report(ctx context.Context, database *bun.DB, inspector *asynq.Inspector, order string, attempted, crashes int) {
	var postIDs []uuid.UUID
	if err := database.NewSelect().Model((*models.Post)(nil)).Column("id").Scan(ctx, &postIDs); err != nil {
		fail("read posts", err)
	}
	inDB := make(map[uuid.UUID]bool, len(postIDs))
	for _, id := range postIDs {
		inDB[id] = true
	}

	inQueue := map[uuid.UUID]bool{}
	for page := 1; ; page++ {
		tasks, err := inspector.ListPendingTasks("default", asynq.PageSize(500), asynq.Page(page))
		if err != nil {
			fail("read jobs", err)
		}
		if len(tasks) == 0 {
			break
		}
		for _, t := range tasks {
			var p jobs.DeliverPostPayload
			if err := json.Unmarshal(t.Payload, &p); err == nil {
				inQueue[p.PostID] = true
			}
		}
	}

	postsWithoutJob := 0
	for id := range inDB {
		if !inQueue[id] {
			postsWithoutJob++
		}
	}
	jobsWithoutPost := 0
	for id := range inQueue {
		if !inDB[id] {
			jobsWithoutPost++
		}
	}

	fmt.Println()
	fmt.Printf("order:              %s\n", order)
	fmt.Printf("posts attempted:    %d\n", attempted)
	fmt.Printf("simulated crashes:  %d\n", crashes)
	fmt.Printf("posts saved:        %d\n", len(inDB))
	fmt.Printf("jobs queued:        %d\n", len(inQueue))
	fmt.Printf("posts with no job:  %d   (saved, but never delivered)\n", postsWithoutJob)
	fmt.Printf("jobs with no post:  %d   (queued for a post that does not exist)\n", jobsWithoutPost)
}

func fail(msg string, err error) {
	slog.Error(msg, "err", err)
	os.Exit(1)
}
