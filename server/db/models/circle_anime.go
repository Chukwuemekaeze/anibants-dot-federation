package models

import (
	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

// CircleAnime records that a Circle is about an anime.
type CircleAnime struct {
	bun.BaseModel `bun:"table:circle_anime"`

	CircleID uuid.UUID `bun:"circle_id,pk,type:uuid"`
	AnimeID  uuid.UUID `bun:"anime_id,pk,type:uuid"`
}
