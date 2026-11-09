-- +goose Up
-- Jobs waiting to be queued in Redis. A row is written in the same transaction
-- as the change that needs the job, and the relay deletes it once queued, so
-- this table only ever holds unsent jobs.
CREATE TABLE outbox (
    -- UUIDv7, so ordering by id is oldest first. Also used as the Asynq job ID.
    id uuid PRIMARY KEY,
    job_type text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE outbox;
