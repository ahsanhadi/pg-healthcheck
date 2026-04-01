package checks

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/pg_healthcheck/internal/config"
)

const g04 = "Long-Running Queries & Lock Contention"

// G04Locks checks for long-running queries and lock contention.
type G04Locks struct{}

func (g *G04Locks) Name() string    { return g04 }
func (g *G04Locks) GroupID() string { return "G04" }

func (g *G04Locks) Run(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) ([]Finding, error) {
	var f []Finding
	f = append(f, g04LongQueries(ctx, db, cfg)...)
	f = append(f, g04IdleInTxAge(ctx, db, cfg)...)
	f = append(f, g04LockBlockerChains(ctx, db)...)
	f = append(f, g04DeadlockCount(ctx, db)...)
	f = append(f, g04StatementTimeout(ctx, db)...)
	f = append(f, g04IdleInTxTimeout(ctx, db)...)
	f = append(f, g04PgStatStatements(ctx, db)...)
	f = append(f, g04TopQueries(ctx, db)...)
	f = append(f, g04LogMinDuration(ctx, db)...)
	f = append(f, g04LockTimeout(ctx, db)...)
	return f, nil
}

// G04-001 active queries older than cfg.LongQueryWarnSec (crit if > cfg.LongQueryCritSec)
func g04LongQueries(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) []Finding {
	const q = `SELECT pid, usename, datname,
		EXTRACT(EPOCH FROM (now() - query_start))::int AS age,
		left(query, 120) AS short_query
		FROM pg_stat_activity
		WHERE state = 'active'
		AND query_start < now() - ($1 * interval '1 second')
		AND query NOT LIKE '%pg_stat_activity%'
		ORDER BY age DESC LIMIT 10`
	rows, err := db.Query(ctx, q, cfg.LongQueryWarnSec)
	if err != nil {
		return []Finding{NewSkip("G04-001", g04, "Long-running queries", err.Error())}
	}
	defer rows.Close()
	var critLines, warnLines []string
	for rows.Next() {
		var pid, age int
		var user, dbn, shortQ string
		_ = rows.Scan(&pid, &user, &dbn, &age, &shortQ)
		line := fmt.Sprintf("PID %d (%ds) %s@%s: %s", pid, age, user, dbn, shortQ)
		if age > cfg.LongQueryCritSec {
			critLines = append(critLines, line)
		} else {
			warnLines = append(warnLines, line)
		}
	}
	var findings []Finding
	if len(critLines) > 0 {
		findings = append(findings, NewCrit("G04-001", g04, "Long-running queries",
			fmt.Sprintf("%d query(ies) running > %ds", len(critLines), cfg.LongQueryCritSec),
			"Investigate and consider pg_cancel_backend() or pg_terminate_backend().",
			strings.Join(critLines, "\n"),
			"https://www.postgresql.org/docs/current/monitoring-stats.html"))
	}
	if len(warnLines) > 0 {
		findings = append(findings, NewWarn("G04-001", g04, "Long-running queries",
			fmt.Sprintf("%d query(ies) running > %ds", len(warnLines), cfg.LongQueryWarnSec),
			"Investigate slow queries; consider statement_timeout.",
			strings.Join(warnLines, "\n"),
			"https://www.postgresql.org/docs/current/monitoring-stats.html"))
	}
	if len(findings) == 0 {
		return []Finding{NewOK("G04-001", g04, "Long-running queries",
			fmt.Sprintf("No queries older than %ds", cfg.LongQueryWarnSec),
			"https://www.postgresql.org/docs/current/monitoring-stats.html")}
	}
	return findings
}

// G04-002 idle-in-transaction age (reuses IdleInTxWarnSec from G01-007 logic, separate check ID)
func g04IdleInTxAge(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) []Finding {
	const q = `SELECT pid, usename, datname,
		EXTRACT(EPOCH FROM (now() - state_change))::int AS age
		FROM pg_stat_activity
		WHERE state = 'idle in transaction'
		ORDER BY age DESC LIMIT 5`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G04-002", g04, "Idle-in-transaction age", err.Error())}
	}
	defer rows.Close()
	var lines []string
	var maxAge int
	for rows.Next() {
		var pid, age int
		var user, dbn string
		_ = rows.Scan(&pid, &user, &dbn, &age)
		lines = append(lines, fmt.Sprintf("PID %d (%ds) %s@%s", pid, age, user, dbn))
		if age > maxAge {
			maxAge = age
		}
	}
	if len(lines) == 0 {
		return []Finding{NewOK("G04-002", g04, "Idle-in-transaction age",
			"No idle-in-transaction sessions",
			"https://www.postgresql.org/docs/current/runtime-config-client.html")}
	}
	obs := fmt.Sprintf("%d idle-in-transaction session(s); oldest %ds", len(lines), maxAge)
	if maxAge > cfg.LongQueryCritSec {
		return []Finding{NewCrit("G04-002", g04, "Idle-in-transaction age", obs,
			"Terminate these sessions or set idle_in_transaction_session_timeout.",
			strings.Join(lines, "\n"),
			"https://www.postgresql.org/docs/current/runtime-config-client.html")}
	}
	return []Finding{NewWarn("G04-002", g04, "Idle-in-transaction age", obs,
		"Set idle_in_transaction_session_timeout='30s'.",
		strings.Join(lines, "\n"),
		"https://www.postgresql.org/docs/current/runtime-config-client.html")}
}

// G04-003 lock blocker chains
func g04LockBlockerChains(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT blocked.pid, blocked.usename, blocking.pid AS blocker_pid,
		blocking.usename AS blocker_user,
		EXTRACT(EPOCH FROM (now() - blocked.query_start))::int AS wait_secs,
		left(blocked.query, 80) AS query
		FROM pg_stat_activity blocked
		JOIN pg_locks bl ON bl.pid = blocked.pid AND NOT bl.granted
		JOIN pg_locks kl ON kl.transactionid = bl.transactionid AND kl.granted
		JOIN pg_stat_activity blocking ON blocking.pid = kl.pid
		ORDER BY wait_secs DESC LIMIT 10`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G04-003", g04, "Lock blocker chains", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var pid, blockerPid, waitSecs int
		var user, blockerUser, query string
		_ = rows.Scan(&pid, &user, &blockerPid, &blockerUser, &waitSecs, &query)
		lines = append(lines, fmt.Sprintf("PID %d (%s) blocked by PID %d (%s) for %ds: %s",
			pid, user, blockerPid, blockerUser, waitSecs, query))
	}
	if len(lines) == 0 {
		return []Finding{NewOK("G04-003", g04, "Lock blocker chains",
			"No lock blocking detected",
			"https://www.postgresql.org/docs/current/view-pg-locks.html")}
	}
	return []Finding{NewWarn("G04-003", g04, "Lock blocker chains",
		fmt.Sprintf("%d blocked session(s)", len(lines)),
		"Investigate blocker PIDs; consider pg_cancel_backend() on blockers.",
		strings.Join(lines, "\n"),
		"https://www.postgresql.org/docs/current/view-pg-locks.html")}
}

// G04-004 deadlock count from pg_stat_database
func g04DeadlockCount(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT coalesce(sum(deadlocks), 0) FROM pg_stat_database`
	var total int64
	if err := db.QueryRow(ctx, q).Scan(&total); err != nil {
		return []Finding{NewSkip("G04-004", g04, "Deadlock count", err.Error())}
	}
	obs := fmt.Sprintf("Total deadlocks across all databases: %d", total)
	if total > 0 {
		return []Finding{NewWarn("G04-004", g04, "Deadlock count", obs,
			"Review application transaction ordering to eliminate deadlocks.",
			"Deadlocks indicate inconsistent lock acquisition order in the application.",
			"https://www.postgresql.org/docs/current/explicit-locking.html#LOCKING-DEADLOCKS")}
	}
	return []Finding{NewOK("G04-004", g04, "Deadlock count", obs,
		"https://www.postgresql.org/docs/current/explicit-locking.html#LOCKING-DEADLOCKS")}
}

// G04-005 statement_timeout = 0
func g04StatementTimeout(ctx context.Context, db *pgxpool.Pool) []Finding {
	var val string
	if err := db.QueryRow(ctx, "SELECT setting FROM pg_settings WHERE name='statement_timeout'").Scan(&val); err != nil {
		return []Finding{NewSkip("G04-005", g04, "statement_timeout", err.Error())}
	}
	if val == "0" {
		return []Finding{NewInfo("G04-005", g04, "statement_timeout",
			"statement_timeout = 0 (disabled)",
			"Consider setting statement_timeout at the role/database level for application users.",
			"No statement timeout means runaway queries can hold locks indefinitely.",
			"https://www.postgresql.org/docs/current/runtime-config-client.html")}
	}
	return []Finding{NewOK("G04-005", g04, "statement_timeout",
		fmt.Sprintf("statement_timeout = %s", val),
		"https://www.postgresql.org/docs/current/runtime-config-client.html")}
}

// G04-006 idle_in_transaction_session_timeout = 0
func g04IdleInTxTimeout(ctx context.Context, db *pgxpool.Pool) []Finding {
	var val string
	if err := db.QueryRow(ctx, "SELECT setting FROM pg_settings WHERE name='idle_in_transaction_session_timeout'").Scan(&val); err != nil {
		return []Finding{NewSkip("G04-006", g04, "idle_in_transaction_session_timeout", err.Error())}
	}
	if val == "0" {
		return []Finding{NewWarn("G04-006", g04, "idle_in_transaction_session_timeout",
			"idle_in_transaction_session_timeout = 0 (disabled)",
			"Set idle_in_transaction_session_timeout='30s' to auto-terminate stale transactions.",
			"Idle transactions hold locks and prevent autovacuum from reclaiming dead tuples.",
			"https://www.postgresql.org/docs/current/runtime-config-client.html")}
	}
	return []Finding{NewOK("G04-006", g04, "idle_in_transaction_session_timeout",
		fmt.Sprintf("idle_in_transaction_session_timeout = %s", val),
		"https://www.postgresql.org/docs/current/runtime-config-client.html")}
}

// G04-007 pg_stat_statements absent
func g04PgStatStatements(ctx context.Context, db *pgxpool.Pool) []Finding {
	var exists bool
	const q = `SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='pg_stat_statements')`
	if err := db.QueryRow(ctx, q).Scan(&exists); err != nil {
		return []Finding{NewSkip("G04-007", g04, "pg_stat_statements extension", err.Error())}
	}
	if !exists {
		return []Finding{NewWarn("G04-007", g04, "pg_stat_statements extension",
			"pg_stat_statements is not installed",
			"Install pg_stat_statements: CREATE EXTENSION pg_stat_statements;",
			"Without pg_stat_statements, query performance analysis is severely limited.",
			"https://www.postgresql.org/docs/current/pgstatstatements.html")}
	}
	return []Finding{NewOK("G04-007", g04, "pg_stat_statements extension",
		"pg_stat_statements is installed",
		"https://www.postgresql.org/docs/current/pgstatstatements.html")}
}

// G04-008 top 10 queries by total_exec_time from pg_stat_statements
func g04TopQueries(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT round(total_exec_time::numeric, 2), calls, round(mean_exec_time::numeric, 2),
		left(query, 100)
		FROM pg_stat_statements
		ORDER BY total_exec_time DESC LIMIT 10`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G04-008", g04, "Top queries by total_exec_time",
			"pg_stat_statements not available: "+err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var totalMs, meanMs float64
		var calls int64
		var shortQ string
		_ = rows.Scan(&totalMs, &calls, &meanMs, &shortQ)
		lines = append(lines, fmt.Sprintf("total=%.0fms calls=%d mean=%.2fms: %s",
			totalMs, calls, meanMs, shortQ))
	}
	if len(lines) == 0 {
		return []Finding{NewOK("G04-008", g04, "Top queries by total_exec_time",
			"No query statistics available yet",
			"https://www.postgresql.org/docs/current/pgstatstatements.html")}
	}
	return []Finding{NewInfo("G04-008", g04, "Top queries by total_exec_time",
		fmt.Sprintf("Top %d queries by cumulative execution time", len(lines)),
		"Optimize high-total-time queries with EXPLAIN ANALYZE.",
		strings.Join(lines, "\n"),
		"https://www.postgresql.org/docs/current/pgstatstatements.html")}
}

// G04-009 log_min_duration_statement = -1
func g04LogMinDuration(ctx context.Context, db *pgxpool.Pool) []Finding {
	var val string
	if err := db.QueryRow(ctx, "SELECT setting FROM pg_settings WHERE name='log_min_duration_statement'").Scan(&val); err != nil {
		return []Finding{NewSkip("G04-009", g04, "log_min_duration_statement", err.Error())}
	}
	if val == "-1" {
		return []Finding{NewInfo("G04-009", g04, "log_min_duration_statement",
			"log_min_duration_statement = -1 (disabled)",
			"Set log_min_duration_statement=1000 to log queries slower than 1 second.",
			"Without slow query logging, identifying performance regressions is difficult.",
			"https://www.postgresql.org/docs/current/runtime-config-logging.html")}
	}
	return []Finding{NewOK("G04-009", g04, "log_min_duration_statement",
		fmt.Sprintf("log_min_duration_statement = %sms", val),
		"https://www.postgresql.org/docs/current/runtime-config-logging.html")}
}

// G04-010 lock_timeout = 0
func g04LockTimeout(ctx context.Context, db *pgxpool.Pool) []Finding {
	var val string
	if err := db.QueryRow(ctx, "SELECT setting FROM pg_settings WHERE name='lock_timeout'").Scan(&val); err != nil {
		return []Finding{NewSkip("G04-010", g04, "lock_timeout", err.Error())}
	}
	if val == "0" {
		return []Finding{NewWarn("G04-010", g04, "lock_timeout",
			"lock_timeout = 0 (disabled)",
			"Set lock_timeout at the role level to prevent long lock waits from cascading.",
			"Unbounded lock waits can cause connection pile-ups under contention.",
			"https://www.postgresql.org/docs/current/runtime-config-client.html")}
	}
	return []Finding{NewOK("G04-010", g04, "lock_timeout",
		fmt.Sprintf("lock_timeout = %s", val),
		"https://www.postgresql.org/docs/current/runtime-config-client.html")}
}
