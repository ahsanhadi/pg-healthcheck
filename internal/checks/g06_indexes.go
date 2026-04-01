package checks

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/pg_healthcheck/internal/config"
)

const g06 = "Index Health"

// G06Indexes checks index health and efficiency.
type G06Indexes struct{}

func (g *G06Indexes) Name() string    { return g06 }
func (g *G06Indexes) GroupID() string { return "G06" }

func (g *G06Indexes) Run(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) ([]Finding, error) {
	var f []Finding
	f = append(f, g06UnusedIndexes(ctx, db)...)
	f = append(f, g06DuplicateIndexes(ctx, db)...)
	f = append(f, g06InvalidIndexes(ctx, db)...)
	f = append(f, g06BloatedIndexes(ctx, db)...)
	f = append(f, g06FKWithoutIndex(ctx, db)...)
	f = append(f, g06PrefixRedundantIndexes(ctx, db)...)
	f = append(f, g06LowCardinalityIndexes(ctx, db)...)
	f = append(f, g06BRINCorrelation(ctx, db)...)
	f = append(f, g06StatsResetDate(ctx, db)...)
	f = append(f, g06TablesWithoutPK(ctx, db)...)
	return f, nil
}

// G06-001 unused indexes (idx_scan=0, not PK/unique)
func g06UnusedIndexes(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT s.schemaname || '.' || s.relname, s.indexrelname,
		pg_relation_size(s.indexrelid) AS idx_bytes
		FROM pg_stat_user_indexes s
		JOIN pg_index i ON i.indexrelid = s.indexrelid
		WHERE s.idx_scan = 0
		AND NOT i.indisunique
		AND NOT i.indisprimary
		AND pg_relation_size(s.indexrelid) > 1048576
		ORDER BY idx_bytes DESC LIMIT 20`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G06-001", g06, "Unused indexes", err.Error())}
	}
	defer rows.Close()
	var lines []string
	var totalBytes int64
	for rows.Next() {
		var tbl, idx string
		var bytes int64
		_ = rows.Scan(&tbl, &idx, &bytes)
		lines = append(lines, fmt.Sprintf("%s.%s (%dMB)", tbl, idx, bytes/1024/1024))
		totalBytes += bytes
	}
	if len(lines) == 0 {
		return []Finding{NewOK("G06-001", g06, "Unused indexes",
			"No unused non-unique indexes found (> 1MB)",
			"https://www.postgresql.org/docs/current/indexes.html")}
	}
	return []Finding{NewInfo("G06-001", g06, "Unused indexes",
		fmt.Sprintf("%d unused index(es) wasting %dMB", len(lines), totalBytes/1024/1024),
		"Consider dropping unused indexes after verifying with pg_stat_reset().",
		strings.Join(lines, "\n"),
		"https://www.postgresql.org/docs/current/indexes.html")}
}

// G06-002 duplicate indexes — same table, same columns, same expression/predicate.
// Uses pg_index.indkey (column attribute numbers) instead of indexdef so the
// comparison is not confused by different index names in the definition string.
func g06DuplicateIndexes(ctx context.Context, db *pgxpool.Pool) []Finding {
	// Use string_agg instead of array_agg so pgx can scan directly into string
	const q = `SELECT string_agg(ci.relname, ', ' ORDER BY ci.relname) AS dup_indexes,
		n.nspname AS schema_name,
		ct.relname AS table_name
		FROM pg_index ix
		JOIN pg_class ct ON ct.oid = ix.indrelid
		JOIN pg_class ci ON ci.oid = ix.indexrelid
		JOIN pg_namespace n ON n.oid = ct.relnamespace
		WHERE n.nspname NOT IN ('pg_catalog','information_schema','spock')
		AND ix.indisvalid
		GROUP BY n.nspname, ct.relname, ix.indkey::text,
		         COALESCE(ix.indexprs::text,''), COALESCE(ix.indpred::text,'')
		HAVING count(*) > 1
		ORDER BY schema_name, table_name`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G06-002", g06, "Duplicate indexes", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var names, schema, tbl string
		_ = rows.Scan(&names, &schema, &tbl)
		lines = append(lines, fmt.Sprintf("%s.%s: [%s]", schema, tbl, names))
	}
	if len(lines) == 0 {
		return []Finding{NewOK("G06-002", g06, "Duplicate indexes",
			"No duplicate indexes found",
			"https://www.postgresql.org/docs/current/indexes.html")}
	}
	return []Finding{NewWarn("G06-002", g06, "Duplicate indexes",
		fmt.Sprintf("%d set(s) of duplicate indexes", len(lines)),
		"Drop all but one index in each duplicate set.",
		strings.Join(lines, "\n"),
		"https://www.postgresql.org/docs/current/indexes.html")}
}

// G06-003 invalid indexes
func g06InvalidIndexes(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT n.nspname || '.' || c.relname AS table_name,
		i.relname AS index_name
		FROM pg_index x
		JOIN pg_class c ON c.oid = x.indrelid
		JOIN pg_class i ON i.oid = x.indexrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE NOT x.indisvalid
		ORDER BY 1, 2`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G06-003", g06, "Invalid indexes", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var tbl, idx string
		_ = rows.Scan(&tbl, &idx)
		lines = append(lines, fmt.Sprintf("%s: %s", tbl, idx))
	}
	if len(lines) == 0 {
		return []Finding{NewOK("G06-003", g06, "Invalid indexes",
			"No invalid indexes found",
			"https://www.postgresql.org/docs/current/sql-reindex.html")}
	}
	return []Finding{NewCrit("G06-003", g06, "Invalid indexes",
		fmt.Sprintf("%d invalid index(es) found", len(lines)),
		"Run REINDEX on each invalid index or DROP and recreate it.",
		strings.Join(lines, "\n"),
		"https://www.postgresql.org/docs/current/sql-reindex.html")}
}

// G06-004 bloated indexes heuristic (> 100MB and size > n_live_tup * 250 bytes)
func g06BloatedIndexes(ctx context.Context, db *pgxpool.Pool) []Finding {
	// n_live_tup lives in pg_stat_user_tables, not pg_stat_user_indexes — must JOIN
	const q = `SELECT i.schemaname || '.' || i.relname AS tbl,
		i.indexrelname AS idx,
		pg_relation_size(i.indexrelid) AS idx_bytes,
		t.n_live_tup AS live
		FROM pg_stat_user_indexes i
		JOIN pg_stat_user_tables t ON t.relid = i.relid
		WHERE pg_relation_size(i.indexrelid) > 104857600
		AND t.n_live_tup > 0
		AND pg_relation_size(i.indexrelid) > t.n_live_tup * 250
		ORDER BY idx_bytes DESC LIMIT 10`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G06-004", g06, "Bloated indexes", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var tbl, idx string
		var bytes, live int64
		_ = rows.Scan(&tbl, &idx, &bytes, &live)
		lines = append(lines, fmt.Sprintf("%s.%s: %dMB for %d rows",
			tbl, idx, bytes/1024/1024, live))
	}
	if len(lines) == 0 {
		return []Finding{NewOK("G06-004", g06, "Bloated indexes",
			"No significantly bloated indexes detected",
			"https://www.postgresql.org/docs/current/routine-vacuuming.html")}
	}
	return []Finding{NewWarn("G06-004", g06, "Bloated indexes",
		fmt.Sprintf("%d potentially bloated index(es)", len(lines)),
		"REINDEX CONCURRENTLY to rebuild bloated indexes without blocking reads.",
		strings.Join(lines, "\n"),
		"https://www.postgresql.org/docs/current/routine-vacuuming.html")}
}

// G06-005 FK columns without a supporting index
func g06FKWithoutIndex(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT c.conrelid::regclass::text AS table_name,
		string_agg(a.attname, ', ' ORDER BY x.n) AS columns
		FROM pg_constraint c
		CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS x(attnum, n)
		JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = x.attnum
		WHERE c.contype = 'f'
		AND NOT EXISTS (
			SELECT 1 FROM pg_index i
			WHERE i.indrelid = c.conrelid
			AND (i.indkey::int[])[0:array_length(c.conkey,1)-1] @> c.conkey::int[]
		)
		GROUP BY c.conrelid, c.conname
		ORDER BY 1 LIMIT 20`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G06-005", g06, "FK columns without index", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var tbl, cols string
		_ = rows.Scan(&tbl, &cols)
		lines = append(lines, fmt.Sprintf("%s (%s)", tbl, cols))
	}
	if len(lines) == 0 {
		return []Finding{NewOK("G06-005", g06, "FK columns without index",
			"All foreign key columns have supporting indexes",
			"https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-FK")}
	}
	return []Finding{NewWarn("G06-005", g06, "FK columns without index",
		fmt.Sprintf("%d FK column set(s) missing a supporting index", len(lines)),
		"CREATE INDEX on the FK column(s) to speed up cascades and joins.",
		strings.Join(lines, "\n"),
		"https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-FK")}
}

// G06-006 prefix-redundant indexes
func g06PrefixRedundantIndexes(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT a.schemaname, a.tablename, a.indexname AS shorter,
		b.indexname AS longer
		FROM pg_indexes a
		JOIN pg_indexes b ON b.tablename = a.tablename
		AND b.schemaname = a.schemaname
		AND b.indexname <> a.indexname
		WHERE position(replace(a.indexdef, a.indexname, '') IN
			replace(b.indexdef, b.indexname, '')) = 1
		AND a.schemaname NOT IN ('pg_catalog','information_schema')
		ORDER BY 1, 2, 3 LIMIT 20`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G06-006", g06, "Prefix-redundant indexes", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var schema, tbl, shorter, longer string
		_ = rows.Scan(&schema, &tbl, &shorter, &longer)
		lines = append(lines, fmt.Sprintf("%s.%s: %s is prefix of %s", schema, tbl, shorter, longer))
	}
	if len(lines) == 0 {
		return []Finding{NewOK("G06-006", g06, "Prefix-redundant indexes",
			"No prefix-redundant indexes detected",
			"https://www.postgresql.org/docs/current/indexes.html")}
	}
	return []Finding{NewInfo("G06-006", g06, "Prefix-redundant indexes",
		fmt.Sprintf("%d potentially prefix-redundant index pair(s)", len(lines)),
		"Review whether the shorter indexes can be dropped.",
		strings.Join(lines, "\n"),
		"https://www.postgresql.org/docs/current/indexes.html")}
}

// G06-007 low-cardinality indexed columns
func g06LowCardinalityIndexes(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT s.schemaname || '.' || s.tablename AS tbl,
		s.attname AS col, s.n_distinct
		FROM pg_stats s
		JOIN pg_indexes i ON i.tablename = s.tablename AND i.schemaname = s.schemaname
		AND i.indexdef LIKE '%(' || s.attname || ')%'
		WHERE s.n_distinct BETWEEN -0.01 AND 0.05
		AND s.n_distinct != 0
		AND s.schemaname NOT IN ('pg_catalog','information_schema')
		ORDER BY s.n_distinct LIMIT 10`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G06-007", g06, "Low-cardinality indexed columns", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var tbl, col string
		var ndistinct float64
		_ = rows.Scan(&tbl, &col, &ndistinct)
		lines = append(lines, fmt.Sprintf("%s.%s (n_distinct=%.4f)", tbl, col, ndistinct))
	}
	if len(lines) == 0 {
		return []Finding{NewOK("G06-007", g06, "Low-cardinality indexed columns",
			"No low-cardinality indexed columns detected",
			"https://www.postgresql.org/docs/current/indexes.html")}
	}
	return []Finding{NewInfo("G06-007", g06, "Low-cardinality indexed columns",
		fmt.Sprintf("%d low-cardinality indexed column(s)", len(lines)),
		"B-tree indexes on boolean/status columns may not be selective enough to be useful.",
		strings.Join(lines, "\n"),
		"https://www.postgresql.org/docs/current/indexes.html")}
}

// G06-008 BRIN indexes on columns with correlation < 0.5
func g06BRINCorrelation(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT s.schemaname || '.' || s.tablename AS tbl,
		s.attname AS col, round(s.correlation::numeric, 3) AS corr,
		i.indexname
		FROM pg_stats s
		JOIN pg_indexes i ON i.tablename = s.tablename AND i.schemaname = s.schemaname
		AND i.indexdef LIKE 'CREATE INDEX%USING brin%'
		AND i.indexdef LIKE '%(' || s.attname || ')%'
		WHERE s.correlation IS NOT NULL AND abs(s.correlation) < 0.5
		AND s.schemaname NOT IN ('pg_catalog','information_schema')
		ORDER BY s.correlation LIMIT 10`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G06-008", g06, "BRIN index correlation", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var tbl, col, idx string
		var corr float64
		_ = rows.Scan(&tbl, &col, &corr, &idx)
		lines = append(lines, fmt.Sprintf("%s.%s corr=%.3f idx=%s", tbl, col, corr, idx))
	}
	if len(lines) == 0 {
		return []Finding{NewOK("G06-008", g06, "BRIN index correlation",
			"All BRIN indexes have good physical correlation",
			"https://www.postgresql.org/docs/current/brin-intro.html")}
	}
	return []Finding{NewWarn("G06-008", g06, "BRIN index correlation",
		fmt.Sprintf("%d BRIN index(es) on low-correlation column(s)", len(lines)),
		"Consider replacing BRIN with B-tree for columns with poor physical ordering.",
		strings.Join(lines, "\n"),
		"https://www.postgresql.org/docs/current/brin-intro.html")}
}

// G06-009 stats reset date info — uses pg_stat_bgwriter which always has stats_reset
func g06StatsResetDate(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT stats_reset::text FROM pg_stat_bgwriter`
	var resetTime *string
	if err := db.QueryRow(ctx, q).Scan(&resetTime); err != nil {
		return []Finding{NewSkip("G06-009", g06, "Statistics reset date", err.Error())}
	}
	if resetTime == nil {
		return []Finding{NewInfo("G06-009", g06, "Statistics reset date",
			"Index statistics have never been reset",
			"Statistics accumulate since cluster start; idx_scan=0 may reflect recent creation, not disuse.",
			"",
			"https://www.postgresql.org/docs/current/monitoring-stats.html")}
	}
	return []Finding{NewInfo("G06-009", g06, "Statistics reset date",
		fmt.Sprintf("Oldest index stats from: %s", *resetTime),
		"Unused index checks are only reliable if stats cover a representative workload period.",
		"",
		"https://www.postgresql.org/docs/current/monitoring-stats.html")}
}

// G06-010 tables without a primary key
// Tables without a PK cannot participate in logical replication or
// Spock/pgEdge replication without REPLICA IDENTITY FULL, which is
// very expensive. They are also harder to uniquely identify rows in.
func g06TablesWithoutPK(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT n.nspname || '.' || c.relname AS tbl,
	            pg_size_pretty(pg_relation_size(c.oid)) AS sz
	            FROM pg_class c
	            JOIN pg_namespace n ON n.oid = c.relnamespace
	            WHERE c.relkind = 'r'
	            AND n.nspname NOT IN ('pg_catalog','information_schema','spock')
	            AND NOT EXISTS (
	                SELECT 1 FROM pg_constraint con
	                WHERE con.conrelid = c.oid AND con.contype = 'p'
	            )
	            ORDER BY pg_relation_size(c.oid) DESC LIMIT 20`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G06-010", g06, "Tables without primary key", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var tbl, sz string
		_ = rows.Scan(&tbl, &sz)
		lines = append(lines, fmt.Sprintf("%-50s  %s", tbl, sz))
	}
	if len(lines) == 0 {
		return []Finding{NewOK("G06-010", g06, "Tables without primary key",
			"All user tables have a primary key",
			"https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-PRIMARY-KEYS")}
	}
	return []Finding{NewWarn("G06-010", g06, "Tables without primary key",
		fmt.Sprintf("%d table(s) have no primary key", len(lines)),
		"Add a PRIMARY KEY or ALTER TABLE ... REPLICA IDENTITY FULL for logical replication support.",
		strings.Join(lines, "\n"),
		"https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-PRIMARY-KEYS")}
}
