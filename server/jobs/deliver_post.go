// Package jobs defines the background jobs. The API uses it to queue jobs
// and the worker uses it to run them, so both agree on each job's shape.
package jobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/uptrace/bun"

	"github.com/Chukwuemekaeze/anibants-dot-federation/server/db/models"
)

// TypeDeliverPost is the job's name in Redis. The worker uses it to find the handler.
const TypeDeliverPost = "post:deliver"

// DeliverPostPayload is the data the job carries, stored in Redis as JSON.
// Only the ID: the worker reads the post when the job runs, so an edited or
// deleted post is never delivered from a stale copy.
type DeliverPostPayload struct {
	PostID uuid.UUID `json:"post_id"`
}

// NewDeliverPostTask builds a job that delivers one post. It keeps Asynq's
// default of 25 retries, spread over about three weeks, because remote
// servers can be down for days.
func NewDeliverPostTask(postID uuid.UUID) (*asynq.Task, error) {
	payload, err := json.Marshal(DeliverPostPayload{PostID: postID})
	if err != nil {
		return nil, fmt.Errorf("encode deliver post payload: %w", err)
	}
	return asynq.NewTask(TypeDeliverPost, payload), nil
}

// Handlers holds what the job handlers need, like a class with self.db.
// The worker creates one at startup and registers its methods.
type Handlers struct {
	DB *bun.DB
}

// DeliverPost runs the delivery job. Returning nil means done, returning an
// error means retry later. Delivery itself is a stub until federation in layer 3.
func (h *Handlers) DeliverPost(ctx context.Context, task *asynq.Task) error {
	var p DeliverPostPayload
	if err := json.Unmarshal(task.Payload(), &p); err != nil {
		// Bad data will never decode, so retrying is pointless.
		return fmt.Errorf("decode deliver post payload: %v: %w", err, asynq.SkipRetry)
	}

	// Read the post as it is now. Soft-deleted posts are hidden at the ORM
	// level, so a deleted post comes back as "not found".
	var post models.Post
	err := h.DB.NewSelect().Model(&post).Where("id = ?", p.PostID).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		slog.Warn("post missing or deleted, nothing to deliver", "post_id", p.PostID)
		return nil
	}
	if err != nil {
		// Probably a database hiccup, so let Asynq retry.
		return fmt.Errorf("load post %s: %w", p.PostID, err)
	}

	slog.Info("delivered post (stub)", "post_id", post.ID, "body", post.Body)
	return nil
}
