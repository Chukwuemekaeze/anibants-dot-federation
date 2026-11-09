// Package posts holds the post logic, like a service module in a FastAPI app.
// The API will call it later; tools and tests call it now.
package posts

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/uptrace/bun"

	"github.com/Chukwuemekaeze/anibants-dot-federation/server/db/models"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/jobs"
)

// ErrEmptyBody is returned for a post with no text.
var ErrEmptyBody = errors.New("post body is empty")

// CreateInput is what a new post needs.
type CreateInput struct {
	CircleID uuid.UUID
	AuthorID uuid.UUID
	Body     string
}

// Create saves a post, then queues its delivery job.
//
// Deliberately naive: the save and the queuing are two separate steps. If the
// program dies between them, the post exists but is never delivered. Step 3
// exposes this gap and step 4 closes it with the transactional outbox.
func Create(ctx context.Context, db *bun.DB, queue *asynq.Client, in CreateInput) (*models.Post, error) {
	if strings.TrimSpace(in.Body) == "" {
		return nil, ErrEmptyBody
	}

	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("make post id: %w", err)
	}
	post := &models.Post{ID: id, CircleID: in.CircleID, AuthorID: in.AuthorID, Body: in.Body}

	// Step 1: save the post. RETURNING fills in created_at, set by the database.
	if _, err := db.NewInsert().Model(post).Returning("created_at").Exec(ctx); err != nil {
		return nil, fmt.Errorf("save post: %w", err)
	}

	// Step 2: queue its delivery job.
	task, err := jobs.NewDeliverPostTask(post.ID)
	if err != nil {
		return nil, err
	}
	if _, err := queue.EnqueueContext(ctx, task); err != nil {
		return nil, fmt.Errorf("queue delivery for saved post %s: %w", post.ID, err)
	}

	return post, nil
}
