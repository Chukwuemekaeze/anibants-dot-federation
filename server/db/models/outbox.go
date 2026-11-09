package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

// OutboxMessage is one job waiting to be queued in Redis by the relay.
type OutboxMessage struct {
	bun.BaseModel `bun:"table:outbox"`

	ID      uuid.UUID `bun:"id,pk,type:uuid"`
	JobType string    `bun:"job_type,notnull"`
	// Raw JSON bytes, passed to Asynq unchanged.
	Payload   json.RawMessage `bun:"payload,type:jsonb,notnull"`
	CreatedAt time.Time       `bun:"created_at,nullzero,notnull,default:current_timestamp"`
}
