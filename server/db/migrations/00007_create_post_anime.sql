-- +goose Up
CREATE TABLE post_anime (
    post_id uuid NOT NULL REFERENCES posts (id),
    anime_id uuid NOT NULL REFERENCES anime (id),
    PRIMARY KEY (post_id, anime_id)
);

-- Drives the home feed: the newest posts marked with a given anime.
CREATE INDEX post_anime_anime_id_post_id_idx ON post_anime (anime_id, post_id DESC);

-- +goose Down
DROP TABLE post_anime;
