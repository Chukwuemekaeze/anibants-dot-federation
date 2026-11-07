-- +goose Up
CREATE TABLE anime (
    id uuid PRIMARY KEY,
    title text NOT NULL,
    -- Text because we have not picked a catalog source, and sources format IDs differently.
    external_id text NOT NULL UNIQUE
);

-- +goose Down
DROP TABLE anime;
