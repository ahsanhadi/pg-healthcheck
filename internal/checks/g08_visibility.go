package checks

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/pg_healthcheck/internal/config"
)

const g08 = "Visibility Map Integrity"

// G08Visibility checks visibility map integrity.
type G08Visibility struct{}

func (g *G08Visibility) Name() string    { return g08 }
func (g *G08Visibility) GroupID() string { return "G08" }

func (g *G08Visibility) Run(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) ([]Finding, error) {
	var f []Finding
	f = append(f, g08HeapBlksRead(ctx, db)...)
	f = append(f, g08AllVisibleCount(ctx, db)...)
	f = append(f, g08PostCrashVM(ctx, db)...)
	f = append(f, g08PgVisibilityExtension(ctx, db)...)
	f = append(f, g08SuspiciousLowDeadTup(ctx, db)...)
	f = append(f, g08VMIntegrityCheck(ctx, db, cfg)...)
	return f, nil
}

// G08-001 high heap_blks_read vs idx_scan
func g08HeapBlksRead(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT s.schemaname || '.' || s.relname AS tbl,
		io.heap_blks_read, s.idx_scan
		FROM pg_stat_user_tables s
		JOIN pg_statio_user_tables io ON io.relid = s.relid
		WHERE io.heap_blks_read > 1000000
		AND s.idx_scan > 0
		AND io.heap_blks_read > s.idx_scan * 100
		ORDER BY io.heap_blks_read DESC LIMIT 10`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G08-001", g08, "High heap_blks_read", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var tbl string
		var heapRead, idxScan int64
		_ = rows.Scan(&tbl, &heapRead, &idxScan)
		lines = append(lines, fmt.Sprintf("%s: heap_blks_read=%d idx_scan=%d", tbl, heapRead, idxScan))
	}
	if err := rows.Err(); err != nil {
		return []Finding{NewSkip("G08-001", g08, "High heap_blks_read", "scan error: "+err.Error())}
	}
	if len(lines) == 0 {
		return []Finding{NewOK("G08-001", g08, "High heap_blks_read",
			"No tables with disproportionately high heap block reads",
			"https://www.postgresql.org/docs/current/monitoring-stats.html")}
	}
	return []Finding{NewInfo("G08-001", g08, "High heap_blks_read",
		fmt.Sprintf("%d table(s) with high heap block reads relative to index scans", len(lines)),
		"Review whether index scans are effectively reducing heap access; check visibility map freshness.",
		strings.Join(lines, "\n"),
		"https://www.postgresql.org/docs/current/monitoring-stats.html")}
}

// G08-002 relallvisible > relpages*1.1
func g08AllVisibleCount(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT n.nspname || '.' || c.relname AS tbl,
		c.relpages, c.relallvisible
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = 'r'
		AND c.relpages > 0
		AND c.relallvisible > c.relpages * 1.1
		ORDER BY c.relallvisible - c.relpages DESC LIMIT 10`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G08-002", g08, "relallvisible consistency", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var tbl string
		var pages, allVis int
		_ = rows.Scan(&tbl, &pages, &allVis)
		lines = append(lines, fmt.Sprintf("%s: relpages=%d relallvisible=%d", tbl, pages, allVis))
	}
	if err := rows.Err(); err != nil {
		return []Finding{NewSkip("G08-002", g08, "relallvisible consistency", "scan error: "+err.Error())}
	}
	if len(lines) == 0 {
		return []Finding{NewOK("G08-002", g08, "relallvisible consistency",
			"All tables have consistent relallvisible <= relpages",
			"https://www.postgresql.org/docs/current/routine-vacuuming.html")}
	}
	return []Finding{NewWarn("G08-002", g08, "relallvisible consistency",
		fmt.Sprintf("%d table(s) have relallvisible > relpages*1.1", len(lines)),
		"Run VACUUM ANALYZE on these tables to refresh visibility map statistics.",
		strings.Join(lines, "\n"),
		"https://www.postgresql.org/docs/current/routine-vacuuming.html")}
}

// G08-003 post-crash VM warning
func g08PostCrashVM(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT setting FROM pg_settings WHERE name = 'recovery_target_timeline'`
	var val string
	_ = db.QueryRow(ctx, q).Scan(&val)
	// Check if the cluster has recently recovered (advisory check)
	var inRecovery bool
	_ = db.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery)
	if inRecovery {
		return []Finding{NewInfo("G08-003", g08, "Post-crash visibility map advisory",
			"Cluster is currently in recovery mode",
			"After promoting a standby, run VACUUM on critical tables to refresh the visibility map.",
			"The visibility map may not be up-to-date immediately after crash recovery or promotion.",
			"https://www.postgresql.org/docs/current/routine-vacuuming.html")}
	}
	return []Finding{NewInfo("G08-003", g08, "Post-crash visibility map advisory",
		"Cluster is not in recovery mode",
		"After any unclean shutdown, schedule VACUUM ANALYZE to ensure visibility map accuracy.",
		"Visibility map pages are not WAL-logged before PG 9.6 and may be stale after a crash.",
		"https://www.postgresql.org/docs/current/routine-vacuuming.html")}
}

// G08-004 pg_visibility extension check
func g08PgVisibilityExtension(ctx context.Context, db *pgxpool.Pool) []Finding {
	var exists bool
	if err := db.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='pg_visibility')").Scan(&exists); err != nil {
		return []Finding{NewSkip("G08-004", g08, "pg_visibility extension", err.Error())}
	}
	if exists {
		return []Finding{NewInfo("G08-004", g08, "pg_visibility extension",
			"pg_visibility extension is installed",
			"Use pg_check_frozen() and pg_check_visible() periodically to detect VM inconsistencies.",
			"pg_visibility allows sampling the visibility map for individual tables.",
			"https://www.postgresql.org/docs/current/pgvisibility.html")}
	}
	return []Finding{NewInfo("G08-004", g08, "pg_visibility extension",
		"pg_visibility extension is not installed",
		"Install pg_visibility for detailed visibility map diagnostics: CREATE EXTENSION pg_visibility;",
		"",
		"https://www.postgresql.org/docs/current/pgvisibility.html")}
}

// G08-005 n_dead_tup suspiciously low despite high n_tup_upd
func g08SuspiciousLowDeadTup(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT schemaname || '.' || relname AS tbl,
		n_tup_upd, n_tup_del, n_dead_tup, last_autovacuum
		FROM pg_stat_user_tables
		WHERE (n_tup_upd + n_tup_del) > 100000
		AND n_dead_tup < 10
		AND last_autovacuum IS NULL
		ORDER BY n_tup_upd DESC LIMIT 10`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G08-005", g08, "Suspiciously low dead tuple count", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var tbl string
		var upd, del, dead int64
		var lastVac *string
		_ = rows.Scan(&tbl, &upd, &del, &dead, &lastVac)
		vac := "never"
		if lastVac != nil {
			vac = *lastVac
		}
		lines = append(lines, fmt.Sprintf("%s: upd=%d del=%d dead=%d last_autovacuum=%s",
			tbl, upd, del, dead, vac))
	}
	if err := rows.Err(); err != nil {
		return []Finding{NewSkip("G08-005", g08, "Suspiciously low dead tuple count", "scan error: "+err.Error())}
	}
	if len(lines) == 0 {
		return []Finding{NewOK("G08-005", g08, "Suspiciously low dead tuple count",
			"No anomalies in dead tuple counters",
			"https://www.postgresql.org/docs/current/monitoring-stats.html")}
	}
	return []Finding{NewInfo("G08-005", g08, "Suspiciously low dead tuple count",
		fmt.Sprintf("%d high-write table(s) with suspiciously low dead tuple count", len(lines)),
		"Stats may have been reset; verify with pg_stat_reset() history and autovacuum logs.",
		strings.Join(lines, "\n"),
		"https://www.postgresql.org/docs/current/monitoring-stats.html")}
}

// G08-006 VM integrity check via pg_visibility extension
// pg_check_visible() finds heap pages the VM marks all-visible that still
// contain dead or not-yet-visible tuples — the real file-level VM/heap mismatch
// that autovacuum cannot self-heal.  pg_check_frozen() finds pages the VM
// marks all-frozen that contain tuples not yet frozen.
// Configure tables to check via pg_visibility_table_list in healthcheck.yaml.
func g08VMIntegrityCheck(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) []Finding {
	var installed bool
	if err := db.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='pg_visibility')`).Scan(&installed); err != nil {
		return []Finding{NewSkip("G08-006", g08, "VM integrity (pg_visibility)", err.Error())}
	}
	if !installed {
		return []Finding{NewSkip("G08-006", g08, "VM integrity (pg_visibility)",
			"pg_visibility extension not installed — run: CREATE EXTENSION pg_visibility")}
	}
	if len(cfg.PgVisibilityTableList) == 0 {
		return []Finding{NewInfo("G08-006", g08, "VM integrity (pg_visibility)",
			"pg_visibility installed but no tables configured",
			"Add tables to pg_visibility_table_list in healthcheck.yaml to enable page-level VM checks.",
			"",
			"https://www.postgresql.org/docs/current/pgvisibility.html")}
	}

	var critLines []string
	for _, tbl := range cfg.PgVisibilityTableList {
		var visCount, frozenCount int64

		if err := db.QueryRow(ctx,
			`SELECT count(*) FROM pg_check_visible($1::regclass)`, tbl).Scan(&visCount); err != nil {
			critLines = append(critLines, fmt.Sprintf("%s: pg_check_visible error: %v", tbl, err))
			continue
		}
		if err := db.QueryRow(ctx,
			`SELECT count(*) FROM pg_check_frozen($1::regclass)`, tbl).Scan(&frozenCount); err != nil {
			critLines = append(critLines, fmt.Sprintf("%s: pg_check_frozen error: %v", tbl, err))
			continue
		}

		if visCount > 0 || frozenCount > 0 {
			critLines = append(critLines, fmt.Sprintf(
				"%s: %d all-visible violation(s), %d all-frozen violation(s)",
				tbl, visCount, frozenCount))
		}
	}

	if len(critLines) > 0 {
		return []Finding{NewCrit("G08-006", g08, "VM integrity (pg_visibility)",
			fmt.Sprintf("%d table(s) have VM/heap mismatches", len(critLines)),
			"Run VACUUM FREEZE on affected tables; restore from backup if corruption persists.",
			strings.Join(critLines, "\n"),
			"https://www.postgresql.org/docs/current/pgvisibility.html")}
	}
	return []Finding{NewOK("G08-006", g08, "VM integrity (pg_visibility)",
		fmt.Sprintf("All %d configured table(s) passed pg_check_visible and pg_check_frozen",
			len(cfg.PgVisibilityTableList)),
		"https://www.postgresql.org/docs/current/pgvisibility.html")}
}
