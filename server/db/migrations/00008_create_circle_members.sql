-- +goose Up
CREATE TABLE circle_members (
    circle_id uuid NOT NULL REFERENCES circles (id),
    user_id uuid NOT NULL REFERENCES users (id),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (circle_id, user_id)
);

-- The primary key finds a Circle's members. This finds a user's Circles.
CREATE INDEX circle_members_user_id_idx ON circle_members (user_id);

-- +goose Down
DROP TABLE circle_members;
