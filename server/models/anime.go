package models

import (
	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

// Anime is one entry imported from the external catalog.
type Anime struct {
	bun.BaseModel `bun:"table:anime"`

	ID         uuid.UUID `bun:"id,pk,type:uuid"`
	Title      string    `bun:"title,notnull"`
	ExternalID string    `bun:"external_id,notnull"`
}
