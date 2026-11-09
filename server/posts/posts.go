// Package posts holds the post logic, like a service module in a FastAPI app.
// The API will call it later; tools and tests call it now.
package posts

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/uptrace/bun"

	"github.com/Chukwuemekaeze/anibants-dot-federation/server/db/models"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/jobs"
	"github.com/Chukwuemekaeze/anibants-dot-federation/server/outbox"
)

// ErrEmptyBody is returned for a post with no text.
var ErrEmptyBody = errors.New("post body is empty")

// CreateInput is what a new post needs.
type CreateInput struct {
	CircleID uuid.UUID
	AuthorID uuid.UUID
	Body     string
}

// Create saves a post and its delivery job in one transaction, using the
// transactional outbox. Either both are saved or neither is, so a saved post
// is always eventually delivered. It never talks to Redis: the outbox relay
// queues the job later.
func Create(ctx context.Context, db *bun.DB, in CreateInput) (*models.Post, error) {
	if strings.TrimSpace(in.Body) == "" {
		return nil, ErrEmptyBody
	}

	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("make post id: %w", err)
	}
	post := &models.Post{ID: id, CircleID: in.CircleID, AuthorID: in.AuthorID, Body: in.Body}

	task, err := jobs.NewDeliverPostTask(post.ID)
	if err != nil {
		return nil, err
	}

	err = db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		// RETURNING fills in created_at, set by the database.
		if _, err := tx.NewInsert().Model(post).Returning("created_at").Exec(ctx); err != nil {
			return fmt.Errorf("save post: %w", err)
		}
		return outbox.Add(ctx, tx, task)
	})
	if err != nil {
		return nil, err
	}
	return post, nil
}
