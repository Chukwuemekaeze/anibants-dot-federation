package models

import (
	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

// PostAnime marks a post as being about an anime. The home feed is built from these.
type PostAnime struct {
	bun.BaseModel `bun:"table:post_anime"`

	PostID  uuid.UUID `bun:"post_id,pk,type:uuid"`
	AnimeID uuid.UUID `bun:"anime_id,pk,type:uuid"`
}
