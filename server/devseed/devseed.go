// Package devseed creates a fixed set of test data for development:
// one user, one anime and one Circle linked to it. The IDs never change,
// so tools and tests can refer to these rows directly.
package devseed

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/uptrace/bun"

	"github.com/Chukwuemekaeze/anibants-dot-federation/server/db/models"
)

// Fixed IDs. The user's is a UUIDv4 and the rest are UUIDv7, matching the real rules.
var (
	UserID   = uuid.MustParse("00000000-0000-4000-8000-0000000000d1")
	AnimeID  = uuid.MustParse("00000000-0000-7000-8000-0000000000a1")
	CircleID = uuid.MustParse("00000000-0000-7000-8000-0000000000c1")
)

// Seed inserts the test rows in one transaction. Rows that already exist are
// skipped, so running it again changes nothing.
func Seed(ctx context.Context, db *bun.DB) error {
	email := "dev@example.com"
	rows := []any{
		&models.User{ID: UserID, Handle: "dev_user", Email: &email},
		&models.Anime{ID: AnimeID, Title: "Naruto", ExternalID: "dev:naruto"},
		&models.Circle{ID: CircleID, OwnerID: UserID, Name: "Dev Naruto", Slug: "dev_naruto"},
		&models.CircleAnime{CircleID: CircleID, AnimeID: AnimeID},
	}

	return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		for _, row := range rows {
			if _, err := tx.NewInsert().Model(row).On("CONFLICT DO NOTHING").Exec(ctx); err != nil {
				return fmt.Errorf("seed %T: %w", row, err)
			}
		}
		return nil
	})
}
