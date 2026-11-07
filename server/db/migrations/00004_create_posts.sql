-- +goose Up
CREATE TABLE posts (
    id uuid PRIMARY KEY,
    circle_id uuid NOT NULL REFERENCES circles (id),
    author_id uuid NOT NULL REFERENCES users (id),
    body text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    deleted_at timestamptz
);

-- UUIDv7 IDs sort by time, so these list a Circle's or an author's newest posts quickly.
CREATE INDEX posts_circle_id_id_idx ON posts (circle_id, id DESC);
CREATE INDEX posts_author_id_id_idx ON posts (author_id, id DESC);

-- +goose Down
DROP TABLE posts;
