package checks

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/pg_healthcheck/internal/config"
)

const g07 = "TOAST Table & Corruption Detection"

// G07Toast checks TOAST table health and data integrity.
type G07Toast struct{}

func (g *G07Toast) Name() string    { return g07 }
func (g *G07Toast) GroupID() string { return "G07" }

func (g *G07Toast) Run(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) ([]Finding, error) {
	var f []Finding
	f = append(f, g07DataChecksums(ctx, db)...)
	f = append(f, g07ChecksumFailures(ctx, db)...)
	f = append(f, g07TOASTReferences(ctx, db)...)
	// G07-004 intentionally skipped (reserved)
	f = append(f, g07OrphanedTOAST(ctx, db)...)
	f = append(f, g07TOASTSize(ctx, db)...)
	f = append(f, g07Amcheck(ctx, db, cfg)...)
	f = append(f, g07CacheHitRatio(ctx, db)...)
	f = append(f, g07PgCheckRelation(ctx, db)...)
	return f, nil
}

// G07-001 data_checksums = off
func g07DataChecksums(ctx context.Context, db *pgxpool.Pool) []Finding {
	var val string
	if err := db.QueryRow(ctx, "SELECT setting FROM pg_settings WHERE name='data_checksums'").Scan(&val); err != nil {
		return []Finding{NewSkip("G07-001", g07, "data_checksums", err.Error())}
	}
	if val != "on" {
		return []Finding{NewCrit("G07-001", g07, "data_checksums",
			"data_checksums = off",
			"Re-initialize the cluster with --data-checksums or use pg_checksums (offline).",
			"Without checksums, silent data corruption on storage will go undetected.",
			"https://www.postgresql.org/docs/current/app-initdb.html#APP-INITDB-DATA-CHECKSUMS")}
	}
	return []Finding{NewOK("G07-001", g07, "data_checksums", "data_checksums = on",
		"https://www.postgresql.org/docs/current/app-initdb.html#APP-INITDB-DATA-CHECKSUMS")}
}

// G07-002 SUM(checksum_failures) > 0
func g07ChecksumFailures(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT coalesce(sum(checksum_failures), 0) FROM pg_stat_database`
	var total int64
	if err := db.QueryRow(ctx, q).Scan(&total); err != nil {
		return []Finding{NewSkip("G07-002", g07, "Checksum failures", err.Error())}
	}
	if total > 0 {
		return []Finding{NewCrit("G07-002", g07, "Checksum failures",
			fmt.Sprintf("%d checksum failure(s) detected", total),
			"Investigate storage hardware immediately; restore from backup if corruption confirmed.",
			"Checksum failures indicate data corruption on disk.",
			"https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-DATABASE-VIEW")}
	}
	return []Finding{NewOK("G07-002", g07, "Checksum failures",
		"No checksum failures detected",
		"https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-DATABASE-VIEW")}
}

// G07-003 reltoastrelid references non-existent pg_class entry
func g07TOASTReferences(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT n.nspname || '.' || c.relname AS tbl, c.reltoastrelid
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.reltoastrelid != 0
		AND NOT EXISTS (SELECT 1 FROM pg_class t WHERE t.oid = c.reltoastrelid)
		AND c.relkind = 'r'
		ORDER BY 1`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G07-003", g07, "TOAST reference integrity", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var tbl string
		var toastOid uint32
		_ = rows.Scan(&tbl, &toastOid)
		lines = append(lines, fmt.Sprintf("%s (toast oid=%d missing)", tbl, toastOid))
	}
	if err := rows.Err(); err != nil {
		return []Finding{NewSkip("G07-003", g07, "TOAST reference integrity", "scan error: "+err.Error())}
	}
	if len(lines) == 0 {
		return []Finding{NewOK("G07-003", g07, "TOAST reference integrity",
			"All reltoastrelid references are valid",
			"https://www.postgresql.org/docs/current/storage-toast.html")}
	}
	return []Finding{NewCrit("G07-003", g07, "TOAST reference integrity",
		fmt.Sprintf("%d table(s) have dangling TOAST references", len(lines)),
		"This indicates catalog corruption; restore from backup immediately.",
		strings.Join(lines, "\n"),
		"https://www.postgresql.org/docs/current/storage-toast.html")}
}

// G07-005 orphaned TOAST tables
func g07OrphanedTOAST(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT n.nspname || '.' || t.relname AS toast_table
		FROM pg_class t
		JOIN pg_namespace n ON n.oid = t.relnamespace
		WHERE t.relkind = 't'
		AND NOT EXISTS (
			SELECT 1 FROM pg_class c WHERE c.reltoastrelid = t.oid
		)
		ORDER BY 1`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G07-005", g07, "Orphaned TOAST tables", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var tbl string
		_ = rows.Scan(&tbl)
		lines = append(lines, tbl)
	}
	if err := rows.Err(); err != nil {
		return []Finding{NewSkip("G07-005", g07, "Orphaned TOAST tables", "scan error: "+err.Error())}
	}
	if len(lines) == 0 {
		return []Finding{NewOK("G07-005", g07, "Orphaned TOAST tables",
			"No orphaned TOAST tables found",
			"https://www.postgresql.org/docs/current/storage-toast.html")}
	}
	return []Finding{NewWarn("G07-005", g07, "Orphaned TOAST tables",
		fmt.Sprintf("%d orphaned TOAST table(s)", len(lines)),
		"These TOAST tables have no parent; investigate possible catalog corruption.",
		strings.Join(lines, "\n"),
		"https://www.postgresql.org/docs/current/storage-toast.html")}
}

// G07-006 TOAST size > 2x main table size
func g07TOASTSize(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT n.nspname || '.' || c.relname AS tbl,
		pg_relation_size(c.oid) AS main_bytes,
		pg_relation_size(c.reltoastrelid) AS toast_bytes
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.reltoastrelid != 0
		AND c.relkind = 'r'
		AND pg_relation_size(c.oid) > 0
		AND pg_relation_size(c.reltoastrelid) > pg_relation_size(c.oid) * 2
		AND pg_relation_size(c.reltoastrelid) > 10485760
		ORDER BY toast_bytes DESC LIMIT 10`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G07-006", g07, "TOAST size vs main table", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var tbl string
		var main, toast int64
		_ = rows.Scan(&tbl, &main, &toast)
		lines = append(lines, fmt.Sprintf("%s: main=%dMB toast=%dMB (%.1fx)",
			tbl, main/1024/1024, toast/1024/1024, float64(toast)/float64(main)))
	}
	if err := rows.Err(); err != nil {
		return []Finding{NewSkip("G07-006", g07, "TOAST size vs main table", "scan error: "+err.Error())}
	}
	if len(lines) == 0 {
		return []Finding{NewOK("G07-006", g07, "TOAST size vs main table",
			"No tables with TOAST > 2x main table size",
			"https://www.postgresql.org/docs/current/storage-toast.html")}
	}
	return []Finding{NewWarn("G07-006", g07, "TOAST size vs main table",
		fmt.Sprintf("%d table(s) with TOAST > 2x main table size", len(lines)),
		"Review large object storage; consider ALTER TABLE ... SET STORAGE or archiving.",
		strings.Join(lines, "\n"),
		"https://www.postgresql.org/docs/current/storage-toast.html")}
}

// G07-007 amcheck bt_index_check
func g07Amcheck(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) []Finding {
	// Check if amcheck is installed
	var exists bool
	if err := db.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='amcheck')").Scan(&exists); err != nil || !exists {
		return []Finding{NewSkip("G07-007", g07, "amcheck index verification",
			"amcheck extension not installed; run: CREATE EXTENSION amcheck")}
	}
	if len(cfg.AmcheckTableList) == 0 {
		return []Finding{NewInfo("G07-007", g07, "amcheck index verification",
			"amcheck is installed but no tables configured for checking",
			"Add table names to amcheck_table_list in healthcheck.yaml.",
			"",
			"https://www.postgresql.org/docs/current/amcheck.html")}
	}
	var errLines []string
	for _, tbl := range cfg.AmcheckTableList {
		const q = `SELECT i.relname
			FROM pg_indexes idx
			JOIN pg_class i ON i.relname = idx.indexname
			JOIN pg_class c ON c.relname = idx.tablename
			WHERE idx.tablename = $1 AND idx.indexdef LIKE '%USING btree%'`
		rows, err := db.Query(ctx, q, tbl)
		if err != nil {
			errLines = append(errLines, fmt.Sprintf("%s: query error: %v", tbl, err))
			continue
		}
		var indexes []string
		for rows.Next() {
			var idxName string
			_ = rows.Scan(&idxName)
			indexes = append(indexes, idxName)
		}
		rows.Close()
		for _, idx := range indexes {
			if _, err := db.Exec(ctx, "SELECT bt_index_check($1::regclass)", idx); err != nil {
				errLines = append(errLines, fmt.Sprintf("%s: %v", idx, err))
			}
		}
	}
	if len(errLines) > 0 {
		return []Finding{NewCrit("G07-007", g07, "amcheck index verification",
			fmt.Sprintf("%d index(es) failed amcheck", len(errLines)),
			"Restore affected indexes from backup; the data may be corrupted.",
			strings.Join(errLines, "\n"),
			"https://www.postgresql.org/docs/current/amcheck.html")}
	}
	return []Finding{NewOK("G07-007", g07, "amcheck index verification",
		fmt.Sprintf("All indexes on configured tables passed amcheck (%d tables)", len(cfg.AmcheckTableList)),
		"https://www.postgresql.org/docs/current/amcheck.html")}
}

// G07-008 cache hit ratio < 90%
func g07CacheHitRatio(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT sum(heap_blks_read), sum(heap_blks_hit)
		FROM pg_statio_user_tables`
	var reads, hits int64
	if err := db.QueryRow(ctx, q).Scan(&reads, &hits); err != nil {
		return []Finding{NewSkip("G07-008", g07, "Cache hit ratio", err.Error())}
	}
	total := reads + hits
	if total == 0 {
		return []Finding{NewOK("G07-008", g07, "Cache hit ratio",
			"No I/O data yet",
			"https://www.postgresql.org/docs/current/monitoring-stats.html")}
	}
	pct := hits * 100 / total
	obs := fmt.Sprintf("Cache hit ratio: %d%% (%d hits / %d total reads)", pct, hits, total)
	if pct < 90 {
		return []Finding{NewInfo("G07-008", g07, "Cache hit ratio", obs,
			"Increase shared_buffers or effective_cache_size; review query patterns.",
			"Low cache hit ratio means frequent disk I/O which degrades performance.",
			"https://www.postgresql.org/docs/current/runtime-config-resource.html")}
	}
	return []Finding{NewOK("G07-008", g07, "Cache hit ratio", obs,
		"https://www.postgresql.org/docs/current/runtime-config-resource.html")}
}

// G07-009 verify_heapam availability (amcheck, PG 13+)
func g07PgCheckRelation(ctx context.Context, db *pgxpool.Pool) []Finding {
	var exists bool
	err := db.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE p.proname = 'verify_heapam' AND n.nspname = 'pg_catalog')").Scan(&exists)
	if err != nil {
		return []Finding{NewSkip("G07-009", g07, "verify_heapam availability", err.Error())}
	}
	if !exists {
		return []Finding{NewInfo("G07-009", g07, "verify_heapam availability",
			"verify_heapam not available — install amcheck to enable heap relation verification",
			"Run: CREATE EXTENSION IF NOT EXISTS amcheck",
			"amcheck's verify_heapam() can detect heap corruption earlier than pg_dump.",
			"https://www.postgresql.org/docs/current/amcheck.html")}
	}
	return []Finding{NewInfo("G07-009", g07, "verify_heapam availability",
		"amcheck verify_heapam() is available",
		"Consider scheduling periodic verify_heapam() calls for critical tables.",
		"verify_heapam() checks heap page and tuple-level consistency without a full dump.",
		"https://www.postgresql.org/docs/current/amcheck.html")}
}
