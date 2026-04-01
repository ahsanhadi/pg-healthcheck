package checks

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/pg_healthcheck/internal/config"
)

const g13 = "OS & Resource-Level Checks"

// G13OSResources checks OS-level and resource-level PostgreSQL metrics.
type G13OSResources struct{}

func (g *G13OSResources) Name() string    { return g13 }
func (g *G13OSResources) GroupID() string { return "G13" }

func (g *G13OSResources) Run(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) ([]Finding, error) {
	var f []Finding
	f = append(f, g13CheckpointSyncTime(ctx, db)...)
	f = append(f, g13PgStatIOEvictions(ctx, db)...)
	f = append(f, g13MaxwrittenClean(ctx, db)...)
	f = append(f, g13HugePages(ctx, db)...)
	f = append(f, g13TempFileSpill(ctx, db)...)
	f = append(f, g13Conflicts(ctx, db)...)
	f = append(f, g13MaxConnections(ctx, db)...)
	return f, nil
}

// G13-001 checkpoint_sync_time / total_checkpoints > 1000ms avg
func g13CheckpointSyncTime(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT checkpoint_sync_time,
		checkpoints_req + checkpoints_timed AS total_chk
		FROM pg_stat_bgwriter`
	var syncTime float64
	var totalChk int64
	if err := db.QueryRow(ctx, q).Scan(&syncTime, &totalChk); err != nil {
		return []Finding{NewSkip("G13-001", g13, "Checkpoint sync time", err.Error())}
	}
	if totalChk == 0 {
		return []Finding{NewOK("G13-001", g13, "Checkpoint sync time",
			"No checkpoints completed yet",
			"https://www.postgresql.org/docs/current/runtime-config-wal.html")}
	}
	avgMs := syncTime / float64(totalChk)
	obs := fmt.Sprintf("avg checkpoint sync time: %.1fms (total checkpoints: %d)", avgMs, totalChk)
	if avgMs > 1000 {
		return []Finding{NewWarn("G13-001", g13, "Checkpoint sync time", obs,
			"Investigate storage I/O throughput; consider increasing checkpoint_completion_target.",
			"High sync time indicates storage cannot flush dirty pages fast enough.",
			"https://www.postgresql.org/docs/current/runtime-config-wal.html")}
	}
	return []Finding{NewOK("G13-001", g13, "Checkpoint sync time", obs,
		"https://www.postgresql.org/docs/current/runtime-config-wal.html")}
}

// G13-002 pg_stat_io evictions on PG 16+
func g13PgStatIOEvictions(ctx context.Context, db *pgxpool.Pool) []Finding {
	var major int
	if err := db.QueryRow(ctx, "SELECT current_setting('server_version_num')::int / 10000").Scan(&major); err != nil {
		return []Finding{NewSkip("G13-002", g13, "pg_stat_io evictions", err.Error())}
	}
	if major < 16 {
		return []Finding{NewInfo("G13-002", g13, "pg_stat_io evictions",
			fmt.Sprintf("PostgreSQL %d — pg_stat_io available in PG 16+", major),
			"Upgrade to PG16+ for detailed I/O statistics including eviction counts.",
			"",
			"https://www.postgresql.org/docs/current/monitoring-stats.html")}
	}
	const q = `SELECT coalesce(sum(evictions), 0) FROM pg_stat_io
		WHERE backend_type = 'client backend'`
	var evictions int64
	if err := db.QueryRow(ctx, q).Scan(&evictions); err != nil {
		return []Finding{NewSkip("G13-002", g13, "pg_stat_io evictions", err.Error())}
	}
	obs := fmt.Sprintf("Total client backend evictions: %d", evictions)
	if evictions > 100000 {
		return []Finding{NewWarn("G13-002", g13, "pg_stat_io evictions", obs,
			"Increase shared_buffers to reduce buffer evictions.",
			"High eviction count means working set does not fit in shared_buffers.",
			"https://www.postgresql.org/docs/current/monitoring-stats.html")}
	}
	return []Finding{NewOK("G13-002", g13, "pg_stat_io evictions", obs,
		"https://www.postgresql.org/docs/current/monitoring-stats.html")}
}

// G13-003 maxwritten_clean > 0
func g13MaxwrittenClean(ctx context.Context, db *pgxpool.Pool) []Finding {
	var maxWrittenClean int64
	if err := db.QueryRow(ctx, "SELECT maxwritten_clean FROM pg_stat_bgwriter").Scan(&maxWrittenClean); err != nil {
		return []Finding{NewSkip("G13-003", g13, "maxwritten_clean", err.Error())}
	}
	obs := fmt.Sprintf("maxwritten_clean = %d", maxWrittenClean)
	if maxWrittenClean > 0 {
		return []Finding{NewWarn("G13-003", g13, "maxwritten_clean", obs,
			"Increase bgwriter_lru_maxpages or shared_buffers to reduce bgwriter scan resets.",
			"maxwritten_clean > 0 means bgwriter is stopping cleaning passes mid-scan.",
			"https://www.postgresql.org/docs/current/runtime-config-resource.html")}
	}
	return []Finding{NewOK("G13-003", g13, "maxwritten_clean", obs,
		"https://www.postgresql.org/docs/current/runtime-config-resource.html")}
}

// G13-004 huge_pages=on advisory
func g13HugePages(ctx context.Context, db *pgxpool.Pool) []Finding {
	var val string
	if err := db.QueryRow(ctx, "SELECT setting FROM pg_settings WHERE name='huge_pages'").Scan(&val); err != nil {
		return []Finding{NewSkip("G13-004", g13, "huge_pages setting", err.Error())}
	}
	obs := fmt.Sprintf("huge_pages = %s", val)
	if val != "on" {
		return []Finding{NewWarn("G13-004", g13, "huge_pages setting", obs,
			"Set huge_pages=on and configure OS huge pages for better memory performance.",
			"Huge pages reduce TLB pressure and improve performance for large shared_buffers.",
			"https://www.postgresql.org/docs/current/kernel-resources.html#LINUX-HUGE-PAGES")}
	}
	return []Finding{NewOK("G13-004", g13, "huge_pages setting", obs,
		"https://www.postgresql.org/docs/current/kernel-resources.html#LINUX-HUGE-PAGES")}
}

// G13-005 temp file spill > 5000 files or > 50GB
func g13TempFileSpill(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT coalesce(sum(temp_files), 0), coalesce(sum(temp_bytes), 0)
		FROM pg_stat_database`
	var files int64
	var bytes int64
	if err := db.QueryRow(ctx, q).Scan(&files, &bytes); err != nil {
		return []Finding{NewSkip("G13-005", g13, "Temp file spill", err.Error())}
	}
	const fiftyGB = int64(50 * 1024 * 1024 * 1024)
	obs := fmt.Sprintf("Total temp files: %d, temp bytes: %dGB", files, bytes/1024/1024/1024)
	if files > 5000 || bytes > fiftyGB {
		return []Finding{NewWarn("G13-005", g13, "Temp file spill", obs,
			"Increase work_mem to reduce disk-based sort and hash operations.",
			"Excessive temp file usage degrades performance and wears on storage.",
			"https://www.postgresql.org/docs/current/runtime-config-resource.html")}
	}
	return []Finding{NewOK("G13-005", g13, "Temp file spill", obs,
		"https://www.postgresql.org/docs/current/runtime-config-resource.html")}
}

// G13-006 conflicts from pg_stat_database > 100
func g13Conflicts(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT coalesce(sum(conflicts), 0) FROM pg_stat_database`
	var total int64
	if err := db.QueryRow(ctx, q).Scan(&total); err != nil {
		return []Finding{NewSkip("G13-006", g13, "Query conflicts", err.Error())}
	}
	obs := fmt.Sprintf("Total query conflicts: %d", total)
	if total > 100 {
		return []Finding{NewWarn("G13-006", g13, "Query conflicts", obs,
			"Enable hot_standby_feedback=on or increase max_standby_streaming_delay.",
			"Conflicts occur when autovacuum or primary activity cancels standby queries.",
			"https://www.postgresql.org/docs/current/hot-standby.html#HOT-STANDBY-CONFLICT")}
	}
	return []Finding{NewOK("G13-006", g13, "Query conflicts", obs,
		"https://www.postgresql.org/docs/current/hot-standby.html")}
}

// G13-007 max_connections > 200
func g13MaxConnections(ctx context.Context, db *pgxpool.Pool) []Finding {
	var maxConn int
	if err := db.QueryRow(ctx, "SELECT setting::int FROM pg_settings WHERE name='max_connections'").Scan(&maxConn); err != nil {
		return []Finding{NewSkip("G13-007", g13, "max_connections advisory", err.Error())}
	}
	obs := fmt.Sprintf("max_connections = %d", maxConn)
	if maxConn > 200 {
		return []Finding{NewInfo("G13-007", g13, "max_connections advisory", obs,
			"Deploy PgBouncer or Pgpool-II for connection pooling to reduce memory overhead.",
			"Each PostgreSQL connection uses ~5-10MB RAM; high max_connections wastes memory even when idle.",
			"https://www.pgbouncer.org/")}
	}
	return []Finding{NewOK("G13-007", g13, "max_connections advisory", obs,
		"https://www.postgresql.org/docs/current/runtime-config-connection.html")}
}
