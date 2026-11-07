-- +goose Up
CREATE TABLE anime_follows (
    user_id uuid NOT NULL REFERENCES users (id),
    anime_id uuid NOT NULL REFERENCES anime (id),
    created_at timestamptz NOT NULL DEFAULT now(),
    -- The pair is the identity, so a user cannot follow the same anime twice.
    PRIMARY KEY (user_id, anime_id)
);

-- The primary key finds a user's anime. This finds an anime's followers.
CREATE INDEX anime_follows_anime_id_idx ON anime_follows (anime_id);

-- +goose Down
DROP TABLE anime_follows;
