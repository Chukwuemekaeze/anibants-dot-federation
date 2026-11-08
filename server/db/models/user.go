package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

// User is a person with an account. The ID is a UUIDv4 so it does not reveal signup time.
type User struct {
	bun.BaseModel `bun:"table:users"`

	ID     uuid.UUID `bun:"id,pk,type:uuid"`
	Handle string    `bun:"handle,notnull"`
	// A pointer so it can be nil (NULL): a deleted account's email is cleared.
	Email     *string   `bun:"email"`
	CreatedAt time.Time `bun:"created_at,nullzero,notnull,default:current_timestamp"`
	// soft_delete: Bun hides deleted users from queries and turns deletes into setting this field.
	DeletedAt time.Time `bun:"deleted_at,soft_delete,nullzero"`
}
