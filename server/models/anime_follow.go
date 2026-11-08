package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

// AnimeFollow records that a user follows an anime.
type AnimeFollow struct {
	bun.BaseModel `bun:"table:anime_follows"`

	// Two pk fields: the pair together is the primary key.
	UserID    uuid.UUID `bun:"user_id,pk,type:uuid"`
	AnimeID   uuid.UUID `bun:"anime_id,pk,type:uuid"`
	CreatedAt time.Time `bun:"created_at,nullzero,notnull,default:current_timestamp"`
}
