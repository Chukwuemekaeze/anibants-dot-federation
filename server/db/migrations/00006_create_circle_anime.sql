-- +goose Up
CREATE TABLE circle_anime (
    circle_id uuid NOT NULL REFERENCES circles (id),
    anime_id uuid NOT NULL REFERENCES anime (id),
    PRIMARY KEY (circle_id, anime_id)
);

-- The primary key finds a Circle's anime. This finds an anime's Circles.
CREATE INDEX circle_anime_anime_id_idx ON circle_anime (anime_id);

-- +goose Down
DROP TABLE circle_anime;
