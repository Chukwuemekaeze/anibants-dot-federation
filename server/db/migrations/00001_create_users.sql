-- +goose Up
CREATE TABLE users (
    id uuid PRIMARY KEY,
    -- UNIQUE covers deleted users too, which is what retires a handle forever.
    handle text NOT NULL UNIQUE CHECK (handle ~ '^[a-z0-9_]{3,30}$'),
    -- Nullable so a deleted account's email can be cleared.
    email text,
    created_at timestamptz NOT NULL DEFAULT now(),
    deleted_at timestamptz,
    CONSTRAINT users_active_needs_email CHECK (deleted_at IS NOT NULL OR email IS NOT NULL)
);

-- Stops Ada@x.com and ada@x.com from being two accounts.
CREATE UNIQUE INDEX users_email_lower_idx ON users (lower(email));

-- +goose Down
DROP TABLE users;
