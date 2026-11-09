package main

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/uptrace/bun"

	"github.com/Chukwuemekaeze/anibants-dot-federation/server/db/models"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/jobs"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/posts"
)

// errSimulatedCrash stands in for the program dying partway through.
var errSimulatedCrash = errors.New("simulated crash")

// One shared Asynq client for the naive modes. Clients are safe to use from
// many goroutines at once. sync.Once makes sure it is created exactly once,
// even when 50 goroutines ask for it at the same moment.
var (
	naiveClient     *asynq.Client
	naiveClientOnce sync.Once
)

func queueFor(opt asynq.RedisClientOpt) *asynq.Client {
	naiveClientOnce.Do(func() {
		naiveClient = asynq.NewClient(opt)
	})
	return naiveClient
}

// createSaveFirst is the naive version that posts.Create used to be:
// save the post, then queue its job, as two separate steps.
func createSaveFirst(ctx context.Context, database *bun.DB, opt asynq.RedisClientOpt, in posts.CreateInput, failRate float64) error {
	post, task, err := newPostAndTask(in)
	if err != nil {
		return err
	}
	if _, err := database.NewInsert().Model(post).Exec(ctx); err != nil {
		return err
	}
	if rand.Float64() < failRate {
		return errSimulatedCrash // saved, but the job is never queued
	}
	_, err = queueFor(opt).EnqueueContext(ctx, task)
	return err
}

// createQueueFirst swaps the two steps: queue the job, then save the post.
func createQueueFirst(ctx context.Context, database *bun.DB, opt asynq.RedisClientOpt, in posts.CreateInput, failRate float64) error {
	post, task, err := newPostAndTask(in)
	if err != nil {
		return err
	}
	if _, err := queueFor(opt).EnqueueContext(ctx, task); err != nil {
		return err
	}
	if rand.Float64() < failRate {
		return errSimulatedCrash // queued, but the post is never saved
	}
	_, err = database.NewInsert().Model(post).Exec(ctx)
	return err
}

func newPostAndTask(in posts.CreateInput) (*models.Post, *asynq.Task, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return nil, nil, err
	}
	task, err := jobs.NewDeliverPostTask(id)
	if err != nil {
		return nil, nil, err
	}
	post := &models.Post{ID: id, CircleID: in.CircleID, AuthorID: in.AuthorID, Body: in.Body}
	return post, task, nil
}
