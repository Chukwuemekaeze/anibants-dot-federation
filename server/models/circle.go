package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

// Circle is a community created by a user and connected to one or more anime.
type Circle struct {
	bun.BaseModel `bun:"table:circles"`

	ID        uuid.UUID `bun:"id,pk,type:uuid"`
	OwnerID   uuid.UUID `bun:"owner_id,type:uuid,notnull"`
	Name      string    `bun:"name,notnull"`
	Slug      string    `bun:"slug,notnull"`
	CreatedAt time.Time `bun:"created_at,nullzero,notnull,default:current_timestamp"`
	DeletedAt time.Time `bun:"deleted_at,soft_delete,nullzero"`
}
