package db

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/Chukwuemekaeze/anibants-dot-federation/server/db/models"
)

func TestOpenRejectsMalformedURL(t *testing.T) {
	_, err := Open(context.Background(), "postgres://user:secret@%%bad")
	if err == nil {
		t.Fatal("expected an error for a malformed URL")
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("error leaks the password: %v", err)
	}
}

// Needs a real database: set DATABASE_URL to run it, otherwise it is skipped.
func TestOpenConnectsAndQueries(t *testing.T) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set")
	}

	ctx := context.Background()
	db, err := Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Proves the connection works and the User model matches the users table.
	if _, err := db.NewSelect().Model((*models.User)(nil)).Count(ctx); err != nil {
		t.Fatalf("query users: %v", err)
	}
}
