-- +goose Up
CREATE TABLE circles (
    id uuid PRIMARY KEY,
    -- Owner rather than creator, because a Circle can be handed over.
    owner_id uuid NOT NULL REFERENCES users (id),
    name text NOT NULL,
    slug text NOT NULL UNIQUE CHECK (slug ~ '^[a-z0-9_]{3,30}$'),
    created_at timestamptz NOT NULL DEFAULT now(),
    deleted_at timestamptz
);

CREATE INDEX circles_owner_id_idx ON circles (owner_id);

-- +goose Down
DROP TABLE circles;
