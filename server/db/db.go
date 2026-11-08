// Package db opens the connection to Postgres. The tables are defined by
// db/migrations and mirrored in Go by db/models.
package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
)

// Open connects to Postgres using a URL such as
// postgres://user:password@localhost:5433/anibants?sslmode=disable
// and checks that the database actually answers before returning.
func Open(ctx context.Context, url string) (*bun.DB, error) {
	connector, err := newConnector(url)
	if err != nil {
		return nil, err
	}

	sqldb := sql.OpenDB(connector)
	// Caps how many connections the pool opens, so a traffic spike cannot
	// use up every connection Postgres allows.
	sqldb.SetMaxOpenConns(20)

	db := bun.NewDB(sqldb, pgdialect.New())

	// Opening a pool does not connect yet, so ping to fail fast on a wrong
	// password, port or database name. Give up after 5 seconds.
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect to database: %w", err)
	}

	return db, nil
}

// newConnector turns the driver's panic on a malformed URL into an ordinary
// error. The panic message is dropped because it can include the password.
func newConnector(url string) (connector *pgdriver.Connector, err error) {
	defer func() {
		if recover() != nil {
			connector = nil
			err = errors.New("database URL is malformed")
		}
	}()
	return pgdriver.NewConnector(pgdriver.WithDSN(url)), nil
}
