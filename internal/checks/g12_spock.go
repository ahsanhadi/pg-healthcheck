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

// spockExists checks if a spock table exists.
func spockExists(ctx context.Context, db *pgxpool.Pool, tableName string) bool {
	var exists bool
	_ = db.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='spock' AND table_name=$1)",
		tableName).Scan(&exists)
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
func g12ApplyLag(ctx context.Context, db *pgxpool.Pool) []Finding {
	if !spockExists(ctx, db, "subscription_status") {
		return []Finding{NewSkip("G12-004", g12, "Spock apply lag",
			"spock.subscription_status not available")}
	}
	const q = `SELECT sub_name,
		EXTRACT(EPOCH FROM (now() - last_event_at))::int AS lag_secs
		FROM spock.subscription_status
		WHERE last_event_at IS NOT NULL
		ORDER BY lag_secs DESC LIMIT 5`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G12-004", g12, "Spock apply lag", err.Error())}
	}
	defer rows.Close()
	var warnLines []string
	for rows.Next() {
		var name string
		var lagSecs int
		_ = rows.Scan(&name, &lagSecs)
		if lagSecs > 300 {
			warnLines = append(warnLines, fmt.Sprintf("%s: %ds lag", name, lagSecs))
		}
	}
	if len(warnLines) > 0 {
		return []Finding{NewWarn("G12-004", g12, "Spock apply lag",
			fmt.Sprintf("%d subscription(s) lagging > 5 minutes", len(warnLines)),
			"Investigate spock worker logs for errors or conflicts.",
			strings.Join(warnLines, "\n"),
			"https://github.com/pgEdge/spock")}
	}
	return []Finding{NewOK("G12-004", g12, "Spock apply lag",
		"All subscriptions applying within 5 minutes",
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
func g12OldExceptions(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) []Finding {
	if !spockExists(ctx, db, "exception_log") {
		return []Finding{NewSkip("G12-007", g12, "Old spock exceptions",
			"spock.exception_log not found")}
	}
	const q = `SELECT count(*) FROM spock.exception_log
		WHERE created_at < now() - ($1 * interval '1 day')`
	var cnt int
	if err := db.QueryRow(ctx, q, cfg.SpockOldExceptionDays).Scan(&cnt); err != nil {
		return []Finding{NewSkip("G12-007", g12, "Old spock exceptions", err.Error())}
	}
	if cnt > 0 {
		cleanupSQL := fmt.Sprintf(
			"-- To remove old exceptions:\n-- DELETE FROM spock.exception_log WHERE created_at < now() - interval '%d days';",
			cfg.SpockOldExceptionDays)
		return []Finding{NewInfo("G12-007", g12, "Old spock exceptions",
			fmt.Sprintf("%d exception(s) older than %d day(s)", cnt, cfg.SpockOldExceptionDays),
			"Periodically clean up old exceptions after confirming they are resolved.",
			cleanupSQL,
			"https://github.com/pgEdge/spock")}
	}
	return []Finding{NewOK("G12-007", g12, "Old spock exceptions",
		fmt.Sprintf("No exceptions older than %d days", cfg.SpockOldExceptionDays),
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
func g12ForwardOrigins(ctx context.Context, db *pgxpool.Pool) []Finding {
	if !spockExists(ctx, db, "subscription") {
		return []Finding{NewSkip("G12-015", g12, "Spock forward_origins",
			"spock not installed")}
	}
	const q = `SELECT sub_name FROM spock.subscription
		WHERE array_length(forward_origins, 1) IS NULL OR array_length(forward_origins, 1) = 0
		ORDER BY 1`
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
func g12SyncState(ctx context.Context, db *pgxpool.Pool) []Finding {
	if !spockExists(ctx, db, "subscription") {
		return []Finding{NewSkip("G12-017", g12, "Spock sync state",
			"spock not installed")}
	}
	const q = `SELECT sub_name, sync_status FROM spock.subscription
		WHERE sync_status NOT IN ('y','r') AND sync_status IS NOT NULL
		ORDER BY 1`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G12-017", g12, "Spock sync state", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var name, status string
		_ = rows.Scan(&name, &status)
		lines = append(lines, fmt.Sprintf("%s: status=%s", name, status))
	}
	if len(lines) > 0 {
		return []Finding{NewWarn("G12-017", g12, "Spock sync state",
			fmt.Sprintf("%d subscription(s) not fully synced", len(lines)),
			"Investigate subscriptions not in 'y' (synced) or 'r' (replicating) state.",
			strings.Join(lines, "\n"),
			"https://github.com/pgEdge/spock")}
	}
	return []Finding{NewOK("G12-017", g12, "Spock sync state",
		"All subscriptions are synced",
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
