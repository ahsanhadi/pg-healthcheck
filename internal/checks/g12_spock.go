package checks

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/pg_healthcheck/internal/config"
)

const g12 = "pgEdge / Spock Cluster"

// G12SpockCluster implements ClusterChecker for pgEdge/Spock multi-master clusters.
type G12SpockCluster struct{}

func (g *G12SpockCluster) Name() string    { return g12 }
func (g *G12SpockCluster) GroupID() string { return "G12" }

// RunCluster executes per-node then cross-node checks.
func (g *G12SpockCluster) RunCluster(ctx context.Context, nodes []*NodeConn, cfg *config.Config) ([]Finding, error) {
	var all []Finding

	// Per-node checks
	for _, node := range nodes {
		var nf []Finding
		nf = append(nf, g12SubEnabled(ctx, node.DB)...)
		nf = append(nf, g12WorkerStatus(ctx, node.DB)...)
		nf = append(nf, g12ApplyLag(ctx, node.DB)...)
		nf = append(nf, g12ExceptionLog(ctx, node.DB, cfg)...)
		nf = append(nf, g12Resolutions(ctx, node.DB, cfg)...)
		nf = append(nf, g12OldExceptions(ctx, node.DB, cfg)...)
		nf = append(nf, g12PgCron(ctx, node.DB)...)
		nf = append(nf, g12SpockWALSlots(ctx, node.DB)...)
		nf = append(nf, g12HotStandbyFeedback(ctx, node.DB)...)
		nf = append(nf, g12WALLevel(ctx, node.DB)...)
		nf = append(nf, g12ForwardOrigins(ctx, node.DB)...)
		nf = append(nf, g12ReplSetMembership(ctx, node.DB)...)
		nf = append(nf, g12SyncState(ctx, node.DB)...)
		nf = append(nf, g12LocalNode(ctx, node.DB)...)
		nf = append(nf, g12LagTracker(ctx, node.DB)...)
		nf = append(nf, g12QueueDepth(ctx, node.DB)...)
		nf = append(nf, g12ChannelStats(ctx, node.DB)...)
		nf = append(nf, g12ReplicationProgress(ctx, node.DB)...)
		all = append(all, tagNode(nf, node.Name)...)
	}

	// Cross-node checks
	all = append(all, g12NodeListConsistency(ctx, nodes)...)
	all = append(all, g12TableParity(ctx, nodes)...)
	all = append(all, g12IndexParity(ctx, nodes)...)
	all = append(all, g12SequenceCollision(ctx, nodes)...)
	all = append(all, g12RowCountSampling(ctx, nodes, cfg)...)

	return all, nil
}

// tagNode sets NodeName on all findings.
func tagNode(findings []Finding, nodeName string) []Finding {
	for i := range findings {
		findings[i].NodeName = nodeName
	}
	return findings
}

// spockExists checks if a spock table or view exists.
func spockExists(ctx context.Context, db *pgxpool.Pool, tableName string) bool {
	var exists bool
	_ = db.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='spock' AND table_name=$1)",
		tableName).Scan(&exists)
	return exists
}

// spockColumnExists checks whether a specific column exists in a spock table.
// Used to handle schema differences across Spock versions gracefully.
func spockColumnExists(ctx context.Context, db *pgxpool.Pool, table, column string) bool {
	var exists bool
	_ = db.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'spock' AND table_name = $1 AND column_name = $2
		)`, table, column).Scan(&exists)
	return exists
}

// G12-002 sub_enabled
func g12SubEnabled(ctx context.Context, db *pgxpool.Pool) []Finding {
	if !spockExists(ctx, db, "subscription") {
		return []Finding{NewSkip("G12-002", g12, "Spock subscriptions enabled",
			"spock.subscription table not found; spock may not be installed")}
	}
	const q = `SELECT sub_name, sub_enabled FROM spock.subscription ORDER BY 1`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G12-002", g12, "Spock subscriptions enabled", err.Error())}
	}
	defer rows.Close()
	var disabled []string
	var total int
	for rows.Next() {
		total++
		var name string
		var enabled bool
		_ = rows.Scan(&name, &enabled)
		if !enabled {
			disabled = append(disabled, name)
		}
	}
	if len(disabled) > 0 {
		return []Finding{NewWarn("G12-002", g12, "Spock subscriptions enabled",
			fmt.Sprintf("%d/%d subscription(s) disabled", len(disabled), total),
			"Re-enable subscriptions: SELECT spock.sub_enable('sub_name');",
			strings.Join(disabled, "\n"),
			"https://github.com/pgEdge/spock")}
	}
	return []Finding{NewOK("G12-002", g12, "Spock subscriptions enabled",
		fmt.Sprintf("All %d subscription(s) enabled", total),
		"https://github.com/pgEdge/spock")}
}

// G12-003 worker status
func g12WorkerStatus(ctx context.Context, db *pgxpool.Pool) []Finding {
	if !spockExists(ctx, db, "subscription") {
		return []Finding{NewSkip("G12-003", g12, "Spock worker status",
			"spock not installed")}
	}
	const q = `SELECT count(*) FROM pg_stat_activity
		WHERE application_name LIKE 'spock%' OR application_name LIKE 'pglogical%'`
	var cnt int
	_ = db.QueryRow(ctx, q).Scan(&cnt)
	if cnt == 0 {
		return []Finding{NewWarn("G12-003", g12, "Spock worker status",
			"No spock worker processes found in pg_stat_activity",
			"Check spock is loaded in shared_preload_libraries and restart PostgreSQL.",
			"",
			"https://github.com/pgEdge/spock")}
	}
	return []Finding{NewOK("G12-003", g12, "Spock worker status",
		fmt.Sprintf("%d spock worker process(es) active", cnt),
		"https://github.com/pgEdge/spock")}
}

// G12-004 apply lag
// Uses spock.lag_tracker (pgEdge Spock). Correct columns confirmed:
//   origin_name, receiver_name, replication_lag_bytes, replication_lag (interval)
func g12ApplyLag(ctx context.Context, db *pgxpool.Pool) []Finding {
	if !spockExists(ctx, db, "lag_tracker") {
		// Fallback: derive lag from pg_replication_slots for spock slots
		const q2 = `SELECT slot_name,
		                    pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes
		             FROM pg_replication_slots
		             WHERE plugin IN ('spock-output','pglogical-output','pgoutput')
		             ORDER BY lag_bytes DESC NULLS LAST`
		rows, err := db.Query(ctx, q2)
		if err != nil {
			return []Finding{NewSkip("G12-004", g12, "Spock apply lag", err.Error())}
		}
		defer rows.Close()
		var warnLines []string
		var total int
		for rows.Next() {
			total++
			var slot string
			var lagBytes int64
			_ = rows.Scan(&slot, &lagBytes)
			if lagBytes > 104857600 {
				warnLines = append(warnLines, fmt.Sprintf("%s: %d MB lag", slot, lagBytes/1024/1024))
			}
		}
		if len(warnLines) > 0 {
			return []Finding{NewWarn("G12-004", g12, "Spock apply lag",
				fmt.Sprintf("%d spock slot(s) lagging > 100 MB", len(warnLines)),
				"Investigate spock worker logs for errors or conflicts.",
				strings.Join(warnLines, "\n"),
				"https://github.com/pgEdge/spock")}
		}
		return []Finding{NewOK("G12-004", g12, "Spock apply lag",
			fmt.Sprintf("All %d spock slot(s) within acceptable lag", total),
			"https://github.com/pgEdge/spock")}
	}

	// spock.lag_tracker columns: origin_name, receiver_name, replication_lag_bytes, replication_lag
	const q = `SELECT origin_name, receiver_name,
	                   COALESCE(replication_lag_bytes, 0)::bigint,
	                   COALESCE(EXTRACT(EPOCH FROM replication_lag), 0)::bigint AS lag_secs
	            FROM spock.lag_tracker
	            ORDER BY replication_lag_bytes DESC NULLS LAST`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G12-004", g12, "Spock apply lag", err.Error())}
	}
	defer rows.Close()
	var warnLines []string
	var total int
	for rows.Next() {
		total++
		var origin, receiver string
		var lagBytes, lagSecs int64
		_ = rows.Scan(&origin, &receiver, &lagBytes, &lagSecs)
		if lagSecs > 300 || lagBytes > 104857600 {
			warnLines = append(warnLines, fmt.Sprintf(
				"%s→%s: %d MB / %ds lag", origin, receiver, lagBytes/1024/1024, lagSecs))
		}
	}
	if len(warnLines) > 0 {
		return []Finding{NewWarn("G12-004", g12, "Spock apply lag",
			fmt.Sprintf("%d receiver(s) lagging > 5min or 100 MB", len(warnLines)),
			"Investigate spock worker logs for errors or network issues.",
			strings.Join(warnLines, "\n"),
			"https://github.com/pgEdge/spock")}
	}
	return []Finding{NewOK("G12-004", g12, "Spock apply lag",
		fmt.Sprintf("All %d receiver(s) within acceptable lag", total),
		"https://github.com/pgEdge/spock")}
}

// G12-005 exception_log
func g12ExceptionLog(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) []Finding {
	if !spockExists(ctx, db, "exception_log") {
		return []Finding{NewSkip("G12-005", g12, "Spock exception log",
			"spock.exception_log not found")}
	}
	const q = `SELECT count(*) FROM spock.exception_log`
	var cnt int
	if err := db.QueryRow(ctx, q).Scan(&cnt); err != nil {
		return []Finding{NewSkip("G12-005", g12, "Spock exception log", err.Error())}
	}
	obs := fmt.Sprintf("%d row(s) in spock.exception_log", cnt)
	cleanupSQL := "-- After reviewing rows, run:\n-- DELETE FROM spock.exception_log WHERE created_at < now() - interval '7 days';"
	if cnt >= cfg.SpockExceptionCritRows {
		return []Finding{NewCrit("G12-005", g12, "Spock exception log", obs,
			fmt.Sprintf("Resolve replication conflicts; exception_log has >= %d rows.", cfg.SpockExceptionCritRows),
			cleanupSQL,
			"https://github.com/pgEdge/spock")}
	}
	if cnt >= cfg.SpockExceptionWarnRows {
		return []Finding{NewWarn("G12-005", g12, "Spock exception log", obs,
			fmt.Sprintf("Review replication conflicts; exception_log has >= %d rows.", cfg.SpockExceptionWarnRows),
			cleanupSQL,
			"https://github.com/pgEdge/spock")}
	}
	return []Finding{NewOK("G12-005", g12, "Spock exception log", obs,
		"https://github.com/pgEdge/spock")}
}

// G12-006 resolutions
func g12Resolutions(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) []Finding {
	if !spockExists(ctx, db, "resolutions") {
		return []Finding{NewSkip("G12-006", g12, "Spock resolutions",
			"spock.resolutions not found")}
	}
	const q = `SELECT count(*) FROM spock.resolutions`
	var cnt int
	if err := db.QueryRow(ctx, q).Scan(&cnt); err != nil {
		return []Finding{NewSkip("G12-006", g12, "Spock resolutions", err.Error())}
	}
	obs := fmt.Sprintf("%d row(s) in spock.resolutions", cnt)
	cleanupSQL := "-- After reviewing rows, run:\n-- DELETE FROM spock.resolutions WHERE created_at < now() - interval '30 days';"
	if cnt >= cfg.SpockResolutionsWarnRows {
		return []Finding{NewWarn("G12-006", g12, "Spock resolutions", obs,
			fmt.Sprintf("High conflict resolution count (>= %d rows); review conflict patterns.", cfg.SpockResolutionsWarnRows),
			cleanupSQL,
			"https://github.com/pgEdge/spock")}
	}
	return []Finding{NewOK("G12-006", g12, "Spock resolutions", obs,
		"https://github.com/pgEdge/spock")}
}

// G12-007 old exceptions
// pgEdge Spock exception_log uses remote_commit_ts as the event timestamp.
// Also joins exception_status to distinguish resolved vs unresolved.
func g12OldExceptions(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) []Finding {
	if !spockExists(ctx, db, "exception_log") {
		return []Finding{NewSkip("G12-007", g12, "Old spock exceptions",
			"spock.exception_log not found")}
	}
	// Count unresolved exceptions older than the configured threshold.
	// Unresolved = no matching row in exception_status, or status != 'resolved'.
	const q = `SELECT count(*) FROM spock.exception_log el
		WHERE el.remote_commit_ts < now() - ($1 * interval '1 day')
		AND NOT EXISTS (
			SELECT 1 FROM spock.exception_status es
			WHERE es.remote_origin     = el.remote_origin
			  AND es.remote_commit_ts  = el.remote_commit_ts
			  AND es.remote_xid        = el.remote_xid
		)`
	var cnt int
	if err := db.QueryRow(ctx, q, cfg.SpockOldExceptionDays).Scan(&cnt); err != nil {
		return []Finding{NewSkip("G12-007", g12, "Old spock exceptions", err.Error())}
	}
	if cnt > 0 {
		cleanupSQL := fmt.Sprintf(
			"DELETE FROM spock.exception_log WHERE remote_commit_ts < now() - interval '%d days';",
			cfg.SpockOldExceptionDays)
		return []Finding{NewWarn("G12-007", g12, "Old spock exceptions",
			fmt.Sprintf("%d unresolved exception(s) older than %d day(s)", cnt, cfg.SpockOldExceptionDays),
			"Review and resolve old exceptions, then clean up.",
			cleanupSQL,
			"https://github.com/pgEdge/spock")}
	}
	return []Finding{NewOK("G12-007", g12, "Old spock exceptions",
		fmt.Sprintf("No unresolved exceptions older than %d days", cfg.SpockOldExceptionDays),
		"https://github.com/pgEdge/spock")}
}

// G12-008 pg_cron
func g12PgCron(ctx context.Context, db *pgxpool.Pool) []Finding {
	var exists bool
	if err := db.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='pg_cron')").Scan(&exists); err != nil {
		return []Finding{NewSkip("G12-008", g12, "pg_cron extension", err.Error())}
	}
	if !exists {
		return []Finding{NewInfo("G12-008", g12, "pg_cron extension",
			"pg_cron is not installed",
			"Install pg_cron for scheduled maintenance tasks on each node.",
			"pg_cron is recommended for automating spock exception_log cleanup.",
			"https://github.com/citusdata/pg_cron")}
	}
	return []Finding{NewOK("G12-008", g12, "pg_cron extension",
		"pg_cron is installed",
		"https://github.com/citusdata/pg_cron")}
}

// G12-009 spock WAL slots
func g12SpockWALSlots(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT slot_name, active,
		pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes
		FROM pg_replication_slots
		WHERE plugin IN ('spock-output','pglogical-output','pgoutput')
		ORDER BY lag_bytes DESC NULLS LAST`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G12-009", g12, "Spock WAL slots", err.Error())}
	}
	defer rows.Close()
	var inactive []string
	var total int
	for rows.Next() {
		total++
		var slot string
		var active bool
		var lagBytes int64
		_ = rows.Scan(&slot, &active, &lagBytes)
		if !active {
			inactive = append(inactive, fmt.Sprintf("%s (lag=%dMB)", slot, lagBytes/1024/1024))
		}
	}
	if len(inactive) > 0 {
		return []Finding{NewWarn("G12-009", g12, "Spock WAL slots",
			fmt.Sprintf("%d/%d spock slot(s) inactive", len(inactive), total),
			"Investigate inactive spock replication slots to prevent WAL accumulation.",
			strings.Join(inactive, "\n"),
			"https://github.com/pgEdge/spock")}
	}
	return []Finding{NewOK("G12-009", g12, "Spock WAL slots",
		fmt.Sprintf("All %d spock slot(s) active", total),
		"https://github.com/pgEdge/spock")}
}

// G12-010 hot_standby_feedback
func g12HotStandbyFeedback(ctx context.Context, db *pgxpool.Pool) []Finding {
	var val string
	if err := db.QueryRow(ctx, "SELECT setting FROM pg_settings WHERE name='hot_standby_feedback'").Scan(&val); err != nil {
		return []Finding{NewSkip("G12-010", g12, "hot_standby_feedback", err.Error())}
	}
	if val != "on" {
		return []Finding{NewWarn("G12-010", g12, "hot_standby_feedback",
			"hot_standby_feedback = off",
			"Enable hot_standby_feedback=on on all spock nodes to prevent query conflicts.",
			"Without this, autovacuum may cancel queries on replica nodes mid-execution.",
			"https://www.postgresql.org/docs/current/runtime-config-replication.html")}
	}
	return []Finding{NewOK("G12-010", g12, "hot_standby_feedback",
		"hot_standby_feedback = on",
		"https://www.postgresql.org/docs/current/runtime-config-replication.html")}
}

// G12-011 wal_level=logical
func g12WALLevel(ctx context.Context, db *pgxpool.Pool) []Finding {
	var val string
	if err := db.QueryRow(ctx, "SELECT setting FROM pg_settings WHERE name='wal_level'").Scan(&val); err != nil {
		return []Finding{NewSkip("G12-011", g12, "wal_level", err.Error())}
	}
	if val != "logical" {
		return []Finding{NewCrit("G12-011", g12, "wal_level",
			fmt.Sprintf("wal_level = %s", val),
			"Set wal_level=logical in postgresql.conf and restart for spock to function.",
			"Spock requires wal_level=logical for logical decoding.",
			"https://www.postgresql.org/docs/current/runtime-config-wal.html")}
	}
	return []Finding{NewOK("G12-011", g12, "wal_level",
		"wal_level = logical",
		"https://www.postgresql.org/docs/current/runtime-config-wal.html")}
}

// G12-015 forward_origins
// Column name differs by Spock version:
//   pgEdge Spock → sub_forward_origins
//   older forks  → forward_origins
func g12ForwardOrigins(ctx context.Context, db *pgxpool.Pool) []Finding {
	if !spockExists(ctx, db, "subscription") {
		return []Finding{NewSkip("G12-015", g12, "Spock forward_origins",
			"spock not installed")}
	}

	col := ""
	for _, candidate := range []string{"sub_forward_origins", "forward_origins"} {
		if spockColumnExists(ctx, db, "subscription", candidate) {
			col = candidate
			break
		}
	}
	if col == "" {
		return []Finding{NewSkip("G12-015", g12, "Spock forward_origins",
			"forward_origins column not found in spock.subscription — may not apply to this Spock version")}
	}

	q := fmt.Sprintf(`SELECT sub_name FROM spock.subscription
		WHERE array_length(%s, 1) IS NULL OR array_length(%s, 1) = 0
		ORDER BY 1`, col, col)
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G12-015", g12, "Spock forward_origins", err.Error())}
	}
	defer rows.Close()
	var found []string
	for rows.Next() {
		var name string
		_ = rows.Scan(&name)
		found = append(found, name)
	}
	if len(found) > 0 {
		return []Finding{NewInfo("G12-015", g12, "Spock forward_origins",
			fmt.Sprintf("%d subscription(s) have empty forward_origins", len(found)),
			"Set forward_origins='{all}' for multi-master topologies to ensure full replication.",
			strings.Join(found, "\n"),
			"https://github.com/pgEdge/spock")}
	}
	return []Finding{NewOK("G12-015", g12, "Spock forward_origins",
		"All subscriptions have forward_origins configured",
		"https://github.com/pgEdge/spock")}
}

// G12-016 replication set membership
func g12ReplSetMembership(ctx context.Context, db *pgxpool.Pool) []Finding {
	if !spockExists(ctx, db, "replication_set_table") {
		return []Finding{NewSkip("G12-016", g12, "Replication set membership",
			"spock.replication_set_table not found")}
	}
	const q = `SELECT count(DISTINCT set_id) AS sets, count(*) AS total_tables
		FROM spock.replication_set_table`
	var sets, tables int
	if err := db.QueryRow(ctx, q).Scan(&sets, &tables); err != nil {
		return []Finding{NewSkip("G12-016", g12, "Replication set membership", err.Error())}
	}
	if tables == 0 {
		return []Finding{NewWarn("G12-016", g12, "Replication set membership",
			"No tables in any replication set",
			"Add tables to a replication set: SELECT spock.replication_set_add_table('default', 'schema.table');",
			"",
			"https://github.com/pgEdge/spock")}
	}
	return []Finding{NewOK("G12-016", g12, "Replication set membership",
		fmt.Sprintf("%d table(s) across %d replication set(s)", tables, sets),
		"https://github.com/pgEdge/spock")}
}

// G12-017 sync state
// Uses spock.local_sync_status — the correct pgEdge Spock table.
// sync_status values: 'i'=initialize, 'd'=data copy, 'f'=finished,
//                     'y'=synced, 'r'=ready (normal), 'e'=error
func g12SyncState(ctx context.Context, db *pgxpool.Pool) []Finding {
	if !spockExists(ctx, db, "local_sync_status") {
		return []Finding{NewSkip("G12-017", g12, "Spock sync state",
			"spock.local_sync_status not found")}
	}
	const q = `SELECT sync_kind, sync_nspname, sync_relname, sync_status
	            FROM spock.local_sync_status
	            WHERE sync_status NOT IN ('r', 'y')
	            ORDER BY sync_status, sync_nspname, sync_relname`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G12-017", g12, "Spock sync state", err.Error())}
	}
	defer rows.Close()
	stateLabel := map[string]string{
		"i": "initialize",
		"d": "data copy",
		"f": "finished copy",
		"e": "error",
	}
	var errLines, inProgLines []string
	for rows.Next() {
		var kind, schema, rel, status string
		_ = rows.Scan(&kind, &schema, &rel, &status)
		obj := schema + "." + rel
		if schema == "" {
			obj = "(global)"
		}
		label := stateLabel[status]
		if label == "" {
			label = status
		}
		line := fmt.Sprintf("kind=%-4s  %-40s  status=%s (%s)", kind, obj, status, label)
		if status == "e" {
			errLines = append(errLines, line)
		} else {
			inProgLines = append(inProgLines, line)
		}
	}
	if len(errLines) > 0 {
		return []Finding{NewCrit("G12-017", g12, "Spock sync state",
			fmt.Sprintf("%d object(s) in error state", len(errLines)),
			"Check PostgreSQL logs; try: SELECT spock.sync_subscription('sub_name');",
			strings.Join(errLines, "\n"),
			"https://github.com/pgEdge/spock")}
	}
	if len(inProgLines) > 0 {
		return []Finding{NewInfo("G12-017", g12, "Spock sync state",
			fmt.Sprintf("%d object(s) still syncing (normal during initial setup)", len(inProgLines)),
			"Monitor until all objects reach 'r' (ready) state.",
			strings.Join(inProgLines, "\n"),
			"https://github.com/pgEdge/spock")}
	}
	return []Finding{NewOK("G12-017", g12, "Spock sync state",
		"All objects in ready/synced state",
		"https://github.com/pgEdge/spock")}
}

// G12-019  spock.local_node registration
// local_node has node_id + node_local_interface only — join node for node_name.
func g12LocalNode(ctx context.Context, db *pgxpool.Pool) []Finding {
	if !spockExists(ctx, db, "local_node") {
		return []Finding{NewSkip("G12-019", g12, "Spock local node registration",
			"spock.local_node not found — Spock may not be fully initialised")}
	}
	const q = `SELECT n.node_name, ln.node_id::text
	            FROM spock.local_node ln
	            JOIN spock.node n ON n.node_id = ln.node_id
	            LIMIT 1`
	var name, id string
	if err := db.QueryRow(ctx, q).Scan(&name, &id); err != nil {
		return []Finding{NewCrit("G12-019", g12, "Spock local node registration",
			"This node is not registered in spock.local_node",
			"Run spock.create_node() to register this node in the cluster.",
			"", "https://github.com/pgEdge/spock")}
	}
	return []Finding{NewOK("G12-019", g12, "Spock local node registration",
		fmt.Sprintf("Registered as node '%s' (id=%s)", name, id),
		"https://github.com/pgEdge/spock")}
}

// G12-020  spock.lag_tracker — per-receiver replication lag snapshot
// Confirmed columns: origin_name, receiver_name, replication_lag_bytes, replication_lag
func g12LagTracker(ctx context.Context, db *pgxpool.Pool) []Finding {
	if !spockExists(ctx, db, "lag_tracker") {
		return []Finding{NewSkip("G12-020", g12, "Spock lag tracker",
			"spock.lag_tracker not available on this Spock version")}
	}
	const q = `SELECT origin_name, receiver_name,
	                   COALESCE(replication_lag_bytes, 0)::bigint,
	                   COALESCE(replication_lag, '0'::interval)::text
	            FROM spock.lag_tracker
	            ORDER BY replication_lag_bytes DESC NULLS LAST`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G12-020", g12, "Spock lag tracker", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var origin, receiver, lagInterval string
		var lagBytes int64
		_ = rows.Scan(&origin, &receiver, &lagBytes, &lagInterval)
		lines = append(lines, fmt.Sprintf("%-15s → %-15s  %d MB  %s",
			origin, receiver, lagBytes/1024/1024, lagInterval))
	}
	if len(lines) == 0 {
		return []Finding{NewOK("G12-020", g12, "Spock lag tracker",
			"No receivers in lag_tracker (no active inbound replication on this node)",
			"https://github.com/pgEdge/spock")}
	}
	return []Finding{NewInfo("G12-020", g12, "Spock lag tracker",
		fmt.Sprintf("%d receiver channel(s) tracked", len(lines)),
		"Monitor for sudden increases — indicates replication pressure or network issues.",
		strings.Join(lines, "\n"),
		"https://github.com/pgEdge/spock")}
}

// G12-021  spock.queue depth
// Confirmed columns: queued_at, role, replication_sets, message_type, message
func g12QueueDepth(ctx context.Context, db *pgxpool.Pool) []Finding {
	if !spockExists(ctx, db, "queue") {
		return []Finding{NewSkip("G12-021", g12, "Spock queue depth",
			"spock.queue not available on this Spock version")}
	}
	const q = `SELECT count(*),
	                   COALESCE(EXTRACT(EPOCH FROM (now() - min(queued_at)))::int, 0) AS oldest_secs
	            FROM spock.queue`
	var cnt int64
	var oldestSecs int64
	if err := db.QueryRow(ctx, q).Scan(&cnt, &oldestSecs); err != nil {
		return []Finding{NewSkip("G12-021", g12, "Spock queue depth", err.Error())}
	}
	obs := fmt.Sprintf("%d message(s) in spock.queue", cnt)
	if cnt > 0 {
		obs = fmt.Sprintf("%d message(s) in spock.queue  (oldest: %ds ago)", cnt, oldestSecs)
	}
	if cnt > 10000 {
		return []Finding{NewWarn("G12-021", g12, "Spock queue depth", obs,
			"Large queue depth suggests subscribers are not consuming messages fast enough.",
			"Check spock worker status and network connectivity to subscriber nodes.",
			"https://github.com/pgEdge/spock")}
	}
	return []Finding{NewOK("G12-021", g12, "Spock queue depth", obs,
		"https://github.com/pgEdge/spock")}
}

// G12-022  channel_summary_stats — per-subscription conflict counts
// Confirmed columns: sub_name, n_tup_ins, n_tup_upd, n_tup_del, n_conflict, n_dca
func g12ChannelStats(ctx context.Context, db *pgxpool.Pool) []Finding {
	if !spockExists(ctx, db, "channel_summary_stats") {
		return []Finding{NewSkip("G12-022", g12, "Spock channel conflict stats",
			"spock.channel_summary_stats not available on this Spock version")}
	}
	const q = `SELECT sub_name, n_tup_ins, n_tup_upd, n_tup_del,
	                   COALESCE(n_conflict, 0) AS n_conflict,
	                   COALESCE(n_dca, 0) AS n_dca
	            FROM spock.channel_summary_stats
	            ORDER BY n_conflict DESC NULLS LAST`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G12-022", g12, "Spock channel conflict stats", err.Error())}
	}
	defer rows.Close()
	var lines []string
	var totalConflicts, totalDCA int64
	for rows.Next() {
		var subName string
		var ins, upd, del, conflicts, dca int64
		_ = rows.Scan(&subName, &ins, &upd, &del, &conflicts, &dca)
		totalConflicts += conflicts
		totalDCA += dca
		lines = append(lines, fmt.Sprintf("%-30s  ins=%-8d upd=%-8d del=%-8d conflicts=%-6d dca=%d",
			subName, ins, upd, del, conflicts, dca))
	}
	if len(lines) == 0 {
		return []Finding{NewInfo("G12-022", g12, "Spock channel conflict stats",
			"No subscription stats yet — stats accumulate as data is replicated",
			"", "", "https://github.com/pgEdge/spock")}
	}
	obs := fmt.Sprintf("%d subscription(s) — total conflicts: %d  DCA: %d",
		len(lines), totalConflicts, totalDCA)
	if totalConflicts > 1000 {
		return []Finding{NewWarn("G12-022", g12, "Spock channel conflict stats", obs,
			"High conflict count indicates write conflicts between nodes — review application logic.",
			strings.Join(lines, "\n"),
			"https://github.com/pgEdge/spock")}
	}
	return []Finding{NewOK("G12-022", g12, "Spock channel conflict stats", obs,
		"https://github.com/pgEdge/spock")}
}

// G12-023  spock.progress — replication position per node pair
// Shows how far behind this node is from each remote node's latest LSN.
func g12ReplicationProgress(ctx context.Context, db *pgxpool.Pool) []Finding {
	if !spockExists(ctx, db, "progress") {
		return []Finding{NewSkip("G12-023", g12, "Spock replication progress",
			"spock.progress not available on this Spock version")}
	}
	const q = `SELECT
	                nl.node_name  AS local_node,
	                nr.node_name  AS remote_node,
	                p.remote_commit_ts,
	                pg_wal_lsn_diff(p.remote_insert_lsn, p.remote_lsn) AS lag_bytes,
	                p.last_updated_ts
	            FROM spock.progress p
	            JOIN spock.node nl ON nl.node_id = p.node_id
	            JOIN spock.node nr ON nr.node_id = p.remote_node_id
	            ORDER BY lag_bytes DESC NULLS LAST`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G12-023", g12, "Spock replication progress", err.Error())}
	}
	defer rows.Close()
	var lines []string
	var maxLagBytes int64
	for rows.Next() {
		var local, remote, commitTs, updatedTs string
		var lagBytes int64
		_ = rows.Scan(&local, &remote, &commitTs, &lagBytes, &updatedTs)
		if lagBytes > maxLagBytes {
			maxLagBytes = lagBytes
		}
		lines = append(lines, fmt.Sprintf("%-12s → %-12s  lag=%d MB  last_commit=%s",
			remote, local, lagBytes/1024/1024, commitTs))
	}
	if len(lines) == 0 {
		return []Finding{NewOK("G12-023", g12, "Spock replication progress",
			"No replication progress entries (no active cluster connections)",
			"https://github.com/pgEdge/spock")}
	}
	obs := fmt.Sprintf("%d node pair(s) tracked  (max lag: %d MB)", len(lines), maxLagBytes/1024/1024)
	if maxLagBytes > 524288000 { // 500 MB
		return []Finding{NewWarn("G12-023", g12, "Spock replication progress", obs,
			"One or more node pairs have significant replication lag.",
			strings.Join(lines, "\n"),
			"https://github.com/pgEdge/spock")}
	}
	return []Finding{NewInfo("G12-023", g12, "Spock replication progress", obs,
		"",
		strings.Join(lines, "\n"),
		"https://github.com/pgEdge/spock")}
}

// ── Cross-node checks ────────────────────────────────────────────────────────

// G12-001 node list consistency
func g12NodeListConsistency(ctx context.Context, nodes []*NodeConn) []Finding {
	if len(nodes) < 2 {
		return []Finding{NewSkip("G12-001", g12, "Node list consistency",
			"Need >= 2 nodes for cross-node comparison")}
	}
	type nodeList []string
	nodeLists := make(map[string]nodeList)
	for _, node := range nodes {
		if !spockExists(ctx, node.DB, "node") {
			continue
		}
		const q = `SELECT node_name FROM spock.node ORDER BY 1`
		rows, err := node.DB.Query(ctx, q)
		if err != nil {
			continue
		}
		var names []string
		for rows.Next() {
			var n string
			_ = rows.Scan(&n)
			names = append(names, n)
		}
		rows.Close()
		nodeLists[node.Name] = names
	}
	if len(nodeLists) < 2 {
		return []Finding{NewSkip("G12-001", g12, "Node list consistency",
			"Could not read node lists from enough nodes")}
	}
	// Compare all node lists against the first
	var refNode string
	var refList nodeList
	for n, l := range nodeLists {
		refNode = n
		refList = l
		break
	}
	var diffs []string
	for n, l := range nodeLists {
		if n == refNode {
			continue
		}
		if strings.Join(refList, ",") != strings.Join(l, ",") {
			diffs = append(diffs, fmt.Sprintf("%s has %v vs %s has %v", n, l, refNode, refList))
		}
	}
	if len(diffs) > 0 {
		return []Finding{NewCrit("G12-001", g12, "Node list consistency",
			fmt.Sprintf("Node lists differ across %d node(s)", len(diffs)),
			"Ensure all nodes have the same spock node membership.",
			strings.Join(diffs, "\n"),
			"https://github.com/pgEdge/spock")}
	}
	return []Finding{NewOK("G12-001", g12, "Node list consistency",
		fmt.Sprintf("All %d nodes have consistent node list", len(nodeLists)),
		"https://github.com/pgEdge/spock")}
}

// G12-012 table parity
func g12TableParity(ctx context.Context, nodes []*NodeConn) []Finding {
	if len(nodes) < 2 {
		return []Finding{NewSkip("G12-012", g12, "Table parity",
			"Need >= 2 nodes for cross-node comparison")}
	}
	const q = `SELECT string_agg(schemaname || '.' || tablename, ',' ORDER BY schemaname, tablename)
		FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema','spock')`
	tableSets := make(map[string]string)
	for _, node := range nodes {
		var tables string
		_ = node.DB.QueryRow(ctx, q).Scan(&tables)
		tableSets[node.Name] = tables
	}
	var diffs []string
	var refNode, refTables string
	for n, t := range tableSets {
		refNode = n
		refTables = t
		break
	}
	for n, t := range tableSets {
		if n == refNode {
			continue
		}
		if t != refTables {
			diffs = append(diffs, fmt.Sprintf("Schema mismatch: %s vs %s", refNode, n))
		}
	}
	if len(diffs) > 0 {
		return []Finding{NewWarn("G12-012", g12, "Table parity",
			fmt.Sprintf("Table schema differs across %d node pair(s)", len(diffs)),
			"Apply DDL migrations to all nodes simultaneously or use spock DDL replication.",
			strings.Join(diffs, "\n"),
			"https://github.com/pgEdge/spock")}
	}
	return []Finding{NewOK("G12-012", g12, "Table parity",
		"All nodes have identical table sets",
		"https://github.com/pgEdge/spock")}
}

// G12-013 index parity
func g12IndexParity(ctx context.Context, nodes []*NodeConn) []Finding {
	if len(nodes) < 2 {
		return []Finding{NewSkip("G12-013", g12, "Index parity",
			"Need >= 2 nodes for cross-node comparison")}
	}
	const q = `SELECT string_agg(indexname || '.' || indexdef, '|' ORDER BY indexname)
		FROM pg_indexes WHERE schemaname NOT IN ('pg_catalog','information_schema','spock')`
	indexSets := make(map[string]string)
	for _, node := range nodes {
		var idxs string
		_ = node.DB.QueryRow(ctx, q).Scan(&idxs)
		indexSets[node.Name] = idxs
	}
	var refNode, refIdxs string
	for n, i := range indexSets {
		refNode = n
		refIdxs = i
		break
	}
	var diffs []string
	for n, i := range indexSets {
		if n == refNode {
			continue
		}
		if i != refIdxs {
			diffs = append(diffs, fmt.Sprintf("Index mismatch: %s vs %s", refNode, n))
		}
	}
	if len(diffs) > 0 {
		return []Finding{NewWarn("G12-013", g12, "Index parity",
			fmt.Sprintf("Index definitions differ across %d node pair(s)", len(diffs)),
			"Ensure all nodes have identical index definitions.",
			strings.Join(diffs, "\n"),
			"https://github.com/pgEdge/spock")}
	}
	return []Finding{NewOK("G12-013", g12, "Index parity",
		"All nodes have identical index definitions",
		"https://github.com/pgEdge/spock")}
}

// G12-014 sequence increment collision
func g12SequenceCollision(ctx context.Context, nodes []*NodeConn) []Finding {
	if len(nodes) < 2 {
		return []Finding{NewSkip("G12-014", g12, "Sequence increment collision",
			"Need >= 2 nodes for cross-node comparison")}
	}
	// Check sequences where increment_by is 1 (risk of collision in multi-master)
	const q = `SELECT schemaname || '.' || sequencename, increment_by
		FROM pg_sequences
		WHERE schemaname NOT IN ('pg_catalog','information_schema')
		AND increment_by = 1
		ORDER BY 1 LIMIT 10`
	var allFindings []Finding
	for _, node := range nodes {
		rows, err := node.DB.Query(ctx, q)
		if err != nil {
			continue
		}
		var seqs []string
		for rows.Next() {
			var seqname string
			var inc int
			_ = rows.Scan(&seqname, &inc)
			seqs = append(seqs, fmt.Sprintf("%s (increment=%d)", seqname, inc))
		}
		rows.Close()
		if len(seqs) > 0 {
			allFindings = append(allFindings, tagNode([]Finding{NewWarn("G12-014", g12, "Sequence increment collision",
				fmt.Sprintf("%d sequence(s) with increment=1 on multi-master cluster", len(seqs)),
				"Use non-overlapping sequence ranges or ddlx_sequence_set_options() to set unique offsets/increments.",
				strings.Join(seqs, "\n"),
				"https://github.com/pgEdge/spock")}, node.Name)...)
		}
	}
	if len(allFindings) == 0 {
		return []Finding{NewOK("G12-014", g12, "Sequence increment collision",
			"No sequences with collision risk detected",
			"https://github.com/pgEdge/spock")}
	}
	return allFindings
}

// G12-018 row count sampling
func g12RowCountSampling(ctx context.Context, nodes []*NodeConn, cfg *config.Config) []Finding {
	if len(nodes) < 2 || len(cfg.ClusterNodes) == 0 {
		return []Finding{NewSkip("G12-018", g12, "Row count sampling",
			"Need >= 2 nodes or no cluster_nodes configured")}
	}
	const q = `SELECT schemaname || '.' || relname, n_live_tup
		FROM pg_stat_user_tables
		WHERE schemaname NOT IN ('pg_catalog','information_schema','spock')
		ORDER BY 1`
	type tableCount struct {
		table string
		count int64
	}
	nodeCounts := make(map[string][]tableCount)
	for _, node := range nodes {
		rows, err := node.DB.Query(ctx, q)
		if err != nil {
			continue
		}
		var counts []tableCount
		for rows.Next() {
			var tbl string
			var cnt int64
			_ = rows.Scan(&tbl, &cnt)
			counts = append(counts, tableCount{tbl, cnt})
		}
		rows.Close()
		nodeCounts[node.Name] = counts
	}
	threshold := cfg.CrossNodeCountThresholdPct
	if threshold <= 0 {
		threshold = 10.0
	}
	var diffs []string
	var refNode string
	var refCounts []tableCount
	for n, c := range nodeCounts {
		refNode = n
		refCounts = c
		break
	}
	for n, counts := range nodeCounts {
		if n == refNode {
			continue
		}
		countMap := make(map[string]int64)
		for _, tc := range counts {
			countMap[tc.table] = tc.count
		}
		for _, ref := range refCounts {
			other, ok := countMap[ref.table]
			if !ok || ref.count == 0 {
				continue
			}
			diff := float64(ref.count-other) / float64(ref.count) * 100
			if diff < 0 {
				diff = -diff
			}
			if diff > threshold {
				diffs = append(diffs, fmt.Sprintf("%s: %s has %d vs %s has %d (%.1f%% diff)",
					ref.table, refNode, ref.count, n, other, diff))
			}
		}
	}
	if len(diffs) > 0 {
		return []Finding{NewWarn("G12-018", g12, "Row count sampling",
			fmt.Sprintf("%d table(s) have significant row count differences across nodes", len(diffs)),
			fmt.Sprintf("Investigate replication lag or data divergence (threshold: %.1f%%).", threshold),
			strings.Join(diffs, "\n"),
			"https://github.com/pgEdge/spock")}
	}
	return []Finding{NewOK("G12-018", g12, "Row count sampling",
		fmt.Sprintf("Row counts consistent across all nodes (threshold: %.1f%%)", threshold),
		"https://github.com/pgEdge/spock")}
}
