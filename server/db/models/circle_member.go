package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

// CircleMember records that a user has joined a Circle.
type CircleMember struct {
	bun.BaseModel `bun:"table:circle_members"`

	CircleID  uuid.UUID `bun:"circle_id,pk,type:uuid"`
	UserID    uuid.UUID `bun:"user_id,pk,type:uuid"`
	CreatedAt time.Time `bun:"created_at,nullzero,notnull,default:current_timestamp"`
}
