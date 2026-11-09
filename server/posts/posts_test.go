package posts

import (
	"context"
	"errors"
	"testing"
)

func TestCreateRejectsEmptyBody(t *testing.T) {
	// The check runs before anything touches the database or Redis,
	// so neither is needed here.
	_, err := Create(context.Background(), nil, nil, CreateInput{Body: "   "})
	if !errors.Is(err, ErrEmptyBody) {
		t.Fatalf("expected ErrEmptyBody, got %v", err)
	}
}
