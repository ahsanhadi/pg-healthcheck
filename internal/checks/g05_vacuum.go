package checks

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/pg_healthcheck/internal/config"
)

const g05 = "Vacuum & Autovacuum Health"

// G05Vacuum checks vacuum and autovacuum health.
type G05Vacuum struct{}

func (g *G05Vacuum) Name() string    { return g05 }
func (g *G05Vacuum) GroupID() string { return "G05" }

func (g *G05Vacuum) Run(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) ([]Finding, error) {
	var f []Finding
	f = append(f, g05DatabaseAge(ctx, db, cfg)...)
	f = append(f, g05TableAge(ctx, db, cfg)...)
	f = append(f, g05DeadTupRatio(ctx, db)...)
	f = append(f, g05LastAutovacuum(ctx, db)...)
	f = append(f, g05AutovacuumDisabled(ctx, db)...)
	f = append(f, g05VacuumScaleFactor(ctx, db)...)
	f = append(f, g05AutovacuumWorkers(ctx, db)...)
	f = append(f, g05VacuumCostDelay(ctx, db)...)
	f = append(f, g05TableBloat(ctx, db)...)
	f = append(f, g05VacuumProgress(ctx, db)...)
	f = append(f, g05AutovacuumWorkMem(ctx, db)...)
	return f, nil
}

// G05-001 pg_database age(datfrozenxid)
func g05DatabaseAge(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) []Finding {
	const q = `SELECT datname, age(datfrozenxid) FROM pg_database
		WHERE datallowconn ORDER BY 2 DESC LIMIT 10`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G05-001", g05, "Database TXID age", err.Error())}
	}
	defer rows.Close()
	const limit = 2100000000
	warnThresh := int64(cfg.TxidWrapWarnMillion) * 1000000
	critThresh := int64(cfg.TxidWrapCritMillion) * 1000000
	var findings []Finding
	for rows.Next() {
		var datname string
		var age int64
		_ = rows.Scan(&datname, &age)
		remaining := int64(limit) - age
		obs := fmt.Sprintf("%s: age=%d, remaining=%d txids", datname, age, remaining)
		if remaining <= critThresh {
			findings = append(findings, NewCrit("G05-001", g05, "Database TXID age", obs,
				"Run VACUUM FREEZE on all tables in this database immediately.",
				"Transaction ID wraparound will cause data loss if not addressed.",
				"https://www.postgresql.org/docs/current/routine-vacuuming.html#VACUUM-FOR-WRAPAROUND"))
		} else if remaining <= warnThresh {
			findings = append(findings, NewWarn("G05-001", g05, "Database TXID age", obs,
				"Schedule VACUUM FREEZE to prevent TXID wraparound.",
				"",
				"https://www.postgresql.org/docs/current/routine-vacuuming.html#VACUUM-FOR-WRAPAROUND"))
		}
	}
	if len(findings) == 0 {
		return []Finding{NewOK("G05-001", g05, "Database TXID age",
			fmt.Sprintf("All databases within safe TXID age thresholds (warn=%dM, crit=%dM)",
				cfg.TxidWrapWarnMillion, cfg.TxidWrapCritMillion),
			"https://www.postgresql.org/docs/current/routine-vacuuming.html#VACUUM-FOR-WRAPAROUND")}
	}
	return findings
}

// G05-002 pg_class age(relfrozenxid) per table
func g05TableAge(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) []Finding {
	const q = `SELECT n.nspname || '.' || c.relname, age(c.relfrozenxid)
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = 'r' AND c.relfrozenxid != 0
		ORDER BY 2 DESC LIMIT 20`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G05-002", g05, "Table TXID age", err.Error())}
	}
	defer rows.Close()
	const limit = 2100000000
	warnThresh := int64(cfg.TxidWrapWarnMillion) * 1000000
	critThresh := int64(cfg.TxidWrapCritMillion) * 1000000
	var critLines, warnLines []string
	for rows.Next() {
		var tbl string
		var age int64
		_ = rows.Scan(&tbl, &age)
		remaining := int64(limit) - age
		line := fmt.Sprintf("%s: age=%d remaining=%d", tbl, age, remaining)
		if remaining <= critThresh {
			critLines = append(critLines, line)
		} else if remaining <= warnThresh {
			warnLines = append(warnLines, line)
		}
	}
	var findings []Finding
	if len(critLines) > 0 {
		findings = append(findings, NewCrit("G05-002", g05, "Table TXID age",
			fmt.Sprintf("%d table(s) approaching TXID wraparound (critical)", len(critLines)),
			"VACUUM FREEZE these tables immediately.",
			strings.Join(critLines, "\n"),
			"https://www.postgresql.org/docs/current/routine-vacuuming.html#VACUUM-FOR-WRAPAROUND"))
	}
	if len(warnLines) > 0 {
		findings = append(findings, NewWarn("G05-002", g05, "Table TXID age",
			fmt.Sprintf("%d table(s) approaching TXID wraparound (warning)", len(warnLines)),
			"Schedule VACUUM FREEZE for these tables.",
			strings.Join(warnLines, "\n"),
			"https://www.postgresql.org/docs/current/routine-vacuuming.html#VACUUM-FOR-WRAPAROUND"))
	}
	if len(findings) == 0 {
		return []Finding{NewOK("G05-002", g05, "Table TXID age",
			"No tables approaching TXID wraparound",
			"https://www.postgresql.org/docs/current/routine-vacuuming.html#VACUUM-FOR-WRAPAROUND")}
	}
	return findings
}

// G05-003 n_dead_tup > n_live_tup*0.2 AND n_live_tup > 10000
func g05DeadTupRatio(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT schemaname || '.' || relname, n_live_tup, n_dead_tup
		FROM pg_stat_user_tables
		WHERE n_live_tup > 10000 AND n_dead_tup > n_live_tup * 0.2
		ORDER BY n_dead_tup DESC LIMIT 10`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G05-003", g05, "Dead tuple ratio", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var tbl string
		var live, dead int64
		_ = rows.Scan(&tbl, &live, &dead)
		pct := dead * 100 / live
		lines = append(lines, fmt.Sprintf("%s: dead=%d live=%d (%d%%)", tbl, dead, live, pct))
	}
	if len(lines) == 0 {
		return []Finding{NewOK("G05-003", g05, "Dead tuple ratio",
			"No tables with dead tuple ratio > 20%",
			"https://www.postgresql.org/docs/current/routine-vacuuming.html")}
	}
	return []Finding{NewWarn("G05-003", g05, "Dead tuple ratio",
		fmt.Sprintf("%d table(s) with dead tuple ratio > 20%%", len(lines)),
		"Run VACUUM on these tables; check autovacuum settings.",
		strings.Join(lines, "\n"),
		"https://www.postgresql.org/docs/current/routine-vacuuming.html")}
}

// G05-004 last_autovacuum older than 7 days for tables with n_tup_upd > 10000
func g05LastAutovacuum(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT schemaname || '.' || relname,
		EXTRACT(EPOCH FROM (now() - last_autovacuum))::int AS age_secs,
		n_tup_upd
		FROM pg_stat_user_tables
		WHERE n_tup_upd > 10000
		AND (last_autovacuum IS NULL OR last_autovacuum < now() - interval '7 days')
		ORDER BY age_secs DESC NULLS FIRST LIMIT 10`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G05-004", g05, "Last autovacuum age", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var tbl string
		var ageSecs *int
		var nUpd int64
		_ = rows.Scan(&tbl, &ageSecs, &nUpd)
		age := "never"
		if ageSecs != nil {
			age = fmt.Sprintf("%ds ago", *ageSecs)
		}
		lines = append(lines, fmt.Sprintf("%s: last_autovacuum=%s updates=%d", tbl, age, nUpd))
	}
	if len(lines) == 0 {
		return []Finding{NewOK("G05-004", g05, "Last autovacuum age",
			"All high-write tables vacuumed within 7 days",
			"https://www.postgresql.org/docs/current/routine-vacuuming.html")}
	}
	return []Finding{NewWarn("G05-004", g05, "Last autovacuum age",
		fmt.Sprintf("%d high-write table(s) not vacuumed in 7+ days", len(lines)),
		"Check autovacuum is running and not being blocked.",
		strings.Join(lines, "\n"),
		"https://www.postgresql.org/docs/current/routine-vacuuming.html")}
}

// G05-005 autovacuum_enabled=false in reloptions
func g05AutovacuumDisabled(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT n.nspname || '.' || c.relname
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.reloptions::text LIKE '%autovacuum_enabled=false%'
		AND c.relkind = 'r'
		ORDER BY 1`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G05-005", g05, "autovacuum disabled tables", err.Error())}
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var tbl string
		_ = rows.Scan(&tbl)
		tables = append(tables, tbl)
	}
	if len(tables) == 0 {
		return []Finding{NewOK("G05-005", g05, "autovacuum disabled tables",
			"No tables have autovacuum explicitly disabled",
			"https://www.postgresql.org/docs/current/routine-vacuuming.html")}
	}
	return []Finding{NewCrit("G05-005", g05, "autovacuum disabled tables",
		fmt.Sprintf("%d table(s) have autovacuum_enabled=false", len(tables)),
		"Re-enable autovacuum unless there is a specific operational reason to disable it.",
		strings.Join(tables, "\n"),
		"https://www.postgresql.org/docs/current/routine-vacuuming.html")}
}

// G05-006 autovacuum_vacuum_scale_factor >= 0.2
func g05VacuumScaleFactor(ctx context.Context, db *pgxpool.Pool) []Finding {
	var val float64
	if err := db.QueryRow(ctx, "SELECT setting::float FROM pg_settings WHERE name='autovacuum_vacuum_scale_factor'").Scan(&val); err != nil {
		return []Finding{NewSkip("G05-006", g05, "autovacuum_vacuum_scale_factor", err.Error())}
	}
	obs := fmt.Sprintf("autovacuum_vacuum_scale_factor = %.3f", val)
	if val >= 0.2 {
		return []Finding{NewInfo("G05-006", g05, "autovacuum_vacuum_scale_factor", obs,
			"Lower autovacuum_vacuum_scale_factor to 0.01-0.05 for large tables.",
			"With 0.2, a 10M-row table won't be vacuumed until 2M dead tuples accumulate.",
			"https://www.postgresql.org/docs/current/runtime-config-autovacuum.html")}
	}
	return []Finding{NewOK("G05-006", g05, "autovacuum_vacuum_scale_factor", obs,
		"https://www.postgresql.org/docs/current/runtime-config-autovacuum.html")}
}

// G05-007 autovacuum_max_workers < 3 with > 10 high-write tables
func g05AutovacuumWorkers(ctx context.Context, db *pgxpool.Pool) []Finding {
	var workers int
	if err := db.QueryRow(ctx, "SELECT setting::int FROM pg_settings WHERE name='autovacuum_max_workers'").Scan(&workers); err != nil {
		return []Finding{NewSkip("G05-007", g05, "autovacuum_max_workers", err.Error())}
	}
	var highWriteTables int
	_ = db.QueryRow(ctx, "SELECT count(*) FROM pg_stat_user_tables WHERE n_tup_upd > 10000").Scan(&highWriteTables)
	obs := fmt.Sprintf("autovacuum_max_workers=%d, high-write tables=%d", workers, highWriteTables)
	if workers < 3 && highWriteTables > 10 {
		return []Finding{NewWarn("G05-007", g05, "autovacuum_max_workers", obs,
			fmt.Sprintf("Increase autovacuum_max_workers to >= 3 (have %d high-write tables).", highWriteTables),
			"",
			"https://www.postgresql.org/docs/current/runtime-config-autovacuum.html")}
	}
	return []Finding{NewOK("G05-007", g05, "autovacuum_max_workers", obs,
		"https://www.postgresql.org/docs/current/runtime-config-autovacuum.html")}
}

// G05-008 autovacuum_vacuum_cost_delay > 10ms
func g05VacuumCostDelay(ctx context.Context, db *pgxpool.Pool) []Finding {
	var val float64
	if err := db.QueryRow(ctx, "SELECT setting::float FROM pg_settings WHERE name='autovacuum_vacuum_cost_delay'").Scan(&val); err != nil {
		return []Finding{NewSkip("G05-008", g05, "autovacuum_vacuum_cost_delay", err.Error())}
	}
	obs := fmt.Sprintf("autovacuum_vacuum_cost_delay = %.1fms", val)
	if val > 10 {
		return []Finding{NewWarn("G05-008", g05, "autovacuum_vacuum_cost_delay", obs,
			"Lower autovacuum_vacuum_cost_delay to 2ms on modern SSDs for faster vacuuming.",
			"High cost delay slows autovacuum, allowing bloat and dead tuple accumulation.",
			"https://www.postgresql.org/docs/current/runtime-config-autovacuum.html")}
	}
	return []Finding{NewOK("G05-008", g05, "autovacuum_vacuum_cost_delay", obs,
		"https://www.postgresql.org/docs/current/runtime-config-autovacuum.html")}
}

// G05-009 table bloat estimate
func g05TableBloat(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT schemaname || '.' || relname,
		pg_total_relation_size(relid) AS total_bytes,
		(n_live_tup + n_dead_tup) AS est_tups
		FROM pg_stat_user_tables
		WHERE (n_live_tup + n_dead_tup) > 0
		AND pg_total_relation_size(relid) > (n_live_tup + n_dead_tup) * 600
		ORDER BY total_bytes DESC LIMIT 10`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G05-009", g05, "Table bloat estimate", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var tbl string
		var totalBytes, estTups int64
		_ = rows.Scan(&tbl, &totalBytes, &estTups)
		lines = append(lines, fmt.Sprintf("%s: size=%dMB est_rows=%d",
			tbl, totalBytes/1024/1024, estTups))
	}
	if len(lines) == 0 {
		return []Finding{NewOK("G05-009", g05, "Table bloat estimate",
			"No tables show excessive bloat",
			"https://www.postgresql.org/docs/current/routine-vacuuming.html")}
	}
	return []Finding{NewWarn("G05-009", g05, "Table bloat estimate",
		fmt.Sprintf("%d table(s) may be significantly bloated", len(lines)),
		"Run VACUUM FULL (or pg_repack) on heavily bloated tables during a maintenance window.",
		strings.Join(lines, "\n"),
		"https://www.postgresql.org/docs/current/routine-vacuuming.html")}
}

// G05-010 pg_stat_progress_vacuum running > 4 hours
func g05VacuumProgress(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT p.pid, n.nspname || '.' || c.relname AS table_name,
		EXTRACT(EPOCH FROM (now() - a.query_start))::int AS age_secs
		FROM pg_stat_progress_vacuum p
		JOIN pg_class c ON c.oid = p.relid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_stat_activity a ON a.pid = p.pid
		WHERE a.query_start < now() - interval '4 hours'`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G05-010", g05, "Long-running vacuum", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var pid, ageSecs int
		var tbl string
		_ = rows.Scan(&pid, &tbl, &ageSecs)
		lines = append(lines, fmt.Sprintf("PID %d on %s (%dh %dm)",
			pid, tbl, ageSecs/3600, (ageSecs%3600)/60))
	}
	if len(lines) == 0 {
		return []Finding{NewOK("G05-010", g05, "Long-running vacuum",
			"No vacuum processes running > 4 hours",
			"https://www.postgresql.org/docs/current/progress-reporting.html")}
	}
	return []Finding{NewInfo("G05-010", g05, "Long-running vacuum",
		fmt.Sprintf("%d vacuum process(es) running > 4 hours", len(lines)),
		"Investigate whether these vacuums are making progress or are blocked.",
		strings.Join(lines, "\n"),
		"https://www.postgresql.org/docs/current/progress-reporting.html")}
}

// G05-011 autovacuum_work_mem=-1 with maintenance_work_mem > 1GB
func g05AutovacuumWorkMem(ctx context.Context, db *pgxpool.Pool) []Finding {
	var avWM, mwm int64
	_ = db.QueryRow(ctx, "SELECT setting::bigint FROM pg_settings WHERE name='autovacuum_work_mem'").Scan(&avWM)
	_ = db.QueryRow(ctx, "SELECT setting::bigint FROM pg_settings WHERE name='maintenance_work_mem'").Scan(&mwm)
	// values in kB; -1 means use maintenance_work_mem
	mwmMB := mwm / 1024
	obs := fmt.Sprintf("autovacuum_work_mem=%d, maintenance_work_mem=%dMB", avWM, mwmMB)
	if avWM == -1 && mwmMB > 1024 {
		return []Finding{NewInfo("G05-011", g05, "autovacuum_work_mem",
			obs,
			"Set autovacuum_work_mem=256MB to prevent each vacuum worker using >1GB of RAM.",
			"With autovacuum_work_mem=-1, each worker uses maintenance_work_mem which may be very large.",
			"https://www.postgresql.org/docs/current/runtime-config-autovacuum.html")}
	}
	return []Finding{NewOK("G05-011", g05, "autovacuum_work_mem", obs,
		"https://www.postgresql.org/docs/current/runtime-config-autovacuum.html")}
}
