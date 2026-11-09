// Package jobs defines the background jobs. The API uses it to queue jobs
// and the worker uses it to run them, so both agree on each job's shape.
package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
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

// HandleDeliverPost runs the job. Returning nil means done, returning an
// error means retry later. It is a stub until federation in layer 3.
func HandleDeliverPost(ctx context.Context, task *asynq.Task) error {
	var p DeliverPostPayload
	if err := json.Unmarshal(task.Payload(), &p); err != nil {
		// Bad data will never decode, so retrying is pointless.
		return fmt.Errorf("decode deliver post payload: %v: %w", err, asynq.SkipRetry)
	}

	slog.Info("delivered post (stub)", "post_id", p.PostID)
	return nil
}
