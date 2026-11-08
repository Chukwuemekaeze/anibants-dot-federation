package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

// Post is a short post inside a Circle.
type Post struct {
	bun.BaseModel `bun:"table:posts"`

	ID        uuid.UUID `bun:"id,pk,type:uuid"`
	CircleID  uuid.UUID `bun:"circle_id,type:uuid,notnull"`
	AuthorID  uuid.UUID `bun:"author_id,type:uuid,notnull"`
	Body      string    `bun:"body,notnull"`
	CreatedAt time.Time `bun:"created_at,nullzero,notnull,default:current_timestamp"`
	DeletedAt time.Time `bun:"deleted_at,soft_delete,nullzero"`
}
