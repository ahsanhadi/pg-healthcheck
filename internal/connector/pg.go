// Package connector opens pgx/v5 connection pools to PostgreSQL instances.
package connector

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/pg-healthcheck/internal/config"
)

// Connect opens a connection pool for the instance described by cfg.
// It falls back to the PGPASSWORD environment variable if no password is set.
func Connect(ctx context.Context, cfg *config.Config) (*pgxpool.Pool, error) {
	password := cfg.Password
	if password == "" {
		password = os.Getenv("PGPASSWORD")
	}

	dsn := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s connect_timeout=%d",
		cfg.Host, cfg.Port, cfg.DBName, cfg.User,
		int(cfg.ConnectionTimeout.Seconds()),
	)
	if password != "" {
		dsn += " password=" + password
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("opening pool to %s:%d: %w", cfg.Host, cfg.Port, err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging %s:%d: %w", cfg.Host, cfg.Port, err)
	}
	return pool, nil
}

// ConnectNode opens a pool to a single cluster node.
func ConnectNode(ctx context.Context, host string, port int, cfg *config.Config) (*pgxpool.Pool, error) {
	clone := *cfg
	clone.Host = host
	clone.Port = port
	return Connect(ctx, &clone)
}
