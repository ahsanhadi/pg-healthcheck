package checks

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"syscall"
	"time"

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
	f = append(f, g13TransparentHugePages()...)
	f = append(f, g13CPUGovernor()...)
	f = append(f, g13DataDirFreeSpace(ctx, db)...)
	f = append(f, g13PostmasterUptime(ctx, db)...)
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

// G13-007 max_connections > 200 (INFO) or > 500 (WARN)
// Values above 500 cause measurable lock manager and memory overhead even when connections
// are idle. pgEdge AI-DBA-Workbench flags >500 as a warning for the same reason.
func g13MaxConnections(ctx context.Context, db *pgxpool.Pool) []Finding {
	var maxConn int
	if err := db.QueryRow(ctx, "SELECT setting::int FROM pg_settings WHERE name='max_connections'").Scan(&maxConn); err != nil {
		return []Finding{NewSkip("G13-007", g13, "max_connections advisory", err.Error())}
	}
	obs := fmt.Sprintf("max_connections = %d", maxConn)
	if maxConn > 500 {
		return []Finding{NewWarn("G13-007", g13, "max_connections advisory", obs,
			"Deploy PgBouncer or Pgpool-II and reduce max_connections to ≤200.",
			"max_connections > 500 degrades lock manager performance and wastes ~5-10MB RAM per slot even when idle.",
			"https://www.pgbouncer.org/")}
	}
	if maxConn > 200 {
		return []Finding{NewInfo("G13-007", g13, "max_connections advisory", obs,
			"Consider deploying PgBouncer or Pgpool-II for connection pooling to reduce memory overhead.",
			"Each PostgreSQL connection uses ~5-10MB RAM; high max_connections wastes memory even when idle.",
			"https://www.pgbouncer.org/")}
	}
	return []Finding{NewOK("G13-007", g13, "max_connections advisory", obs,
		"https://www.postgresql.org/docs/current/runtime-config-connection.html")}
}

// G13-008 Transparent Huge Pages (Linux only)
// THP=always causes random latency spikes in PostgreSQL due to background khugepaged
// compaction stealing CPU and causing stalls. PostgreSQL recommends madvise or never.
func g13TransparentHugePages() []Finding {
	if runtime.GOOS != "linux" {
		return []Finding{NewSkip("G13-008", g13, "Transparent Huge Pages",
			"Check only applicable on Linux")}
	}
	data, err := os.ReadFile("/sys/kernel/mm/transparent_hugepage/enabled")
	if err != nil {
		return []Finding{NewSkip("G13-008", g13, "Transparent Huge Pages",
			fmt.Sprintf("Cannot read THP setting: %v", err))}
	}
	val := strings.TrimSpace(string(data))
	obs := fmt.Sprintf("transparent_hugepage/enabled: %s", val)
	if strings.Contains(val, "[always]") {
		return []Finding{NewWarn("G13-008", g13, "Transparent Huge Pages", obs,
			"Set THP to madvise or never: echo madvise > /sys/kernel/mm/transparent_hugepage/enabled",
			"THP=always causes unpredictable latency spikes in PostgreSQL. The background khugepaged "+
				"compaction process stalls application threads during page coalescence. "+
				"PostgreSQL explicitly recommends against THP=always.",
			"https://www.postgresql.org/docs/current/kernel-resources.html")}
	}
	return []Finding{NewOK("G13-008", g13, "Transparent Huge Pages", obs,
		"https://www.postgresql.org/docs/current/kernel-resources.html")}
}

// G13-009 CPU frequency governor (Linux only)
// The powersave governor throttles CPU frequency to reduce power consumption,
// causing latency spikes and reduced throughput on PostgreSQL workloads.
func g13CPUGovernor() []Finding {
	if runtime.GOOS != "linux" {
		return []Finding{NewSkip("G13-009", g13, "CPU frequency governor",
			"Check only applicable on Linux")}
	}
	data, err := os.ReadFile("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
	if err != nil {
		// cpufreq is not available on many cloud VMs and containers — skip silently.
		return []Finding{NewSkip("G13-009", g13, "CPU frequency governor",
			"cpufreq not available on this system (common in cloud VMs and containers)")}
	}
	val := strings.TrimSpace(string(data))
	obs := fmt.Sprintf("CPU scaling governor: %s", val)
	if val == "powersave" || val == "schedutil" {
		return []Finding{NewWarn("G13-009", g13, "CPU frequency governor", obs,
			"Switch to performance governor: echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor",
			fmt.Sprintf("Governor '%s' throttles CPU frequency under load, causing latency spikes and "+
				"reduced throughput. The 'performance' governor keeps CPU at maximum frequency "+
				"and is strongly recommended for latency-sensitive PostgreSQL workloads.", val),
			"https://www.kernel.org/doc/html/latest/admin-guide/pm/cpufreq.html")}
	}
	return []Finding{NewOK("G13-009", g13, "CPU frequency governor", obs,
		"https://www.kernel.org/doc/html/latest/admin-guide/pm/cpufreq.html")}
}

// G13-010 Data directory filesystem free space
// Queries the PostgreSQL data_directory path and checks available disk space via
// syscall.Statfs. The data directory may be on a different filesystem than pg_wal
// (checked by G14-013), so both checks are needed.
func g13DataDirFreeSpace(ctx context.Context, db *pgxpool.Pool) []Finding {
	var dataDir string
	if err := db.QueryRow(ctx, "SELECT setting FROM pg_settings WHERE name='data_directory'").Scan(&dataDir); err != nil {
		return []Finding{NewSkip("G13-010", g13, "Data directory disk space", err.Error())}
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dataDir, &stat); err != nil {
		if os.IsPermission(err) {
			return []Finding{NewInfo("G13-010", g13, "Data directory disk space",
				fmt.Sprintf("Permission denied reading filesystem stats for %s", dataDir),
				"Run pg_healthcheck as the postgres OS user or grant read access to the data directory.",
				fmt.Sprintf("syscall.Statfs error: %v", err),
				"https://www.postgresql.org/docs/current/storage-file-layout.html")}
		}
		return []Finding{NewInfo("G13-010", g13, "Data directory disk space",
			fmt.Sprintf("Filesystem stat requires local execution (data_directory: %s)", dataDir),
			"Run pg_healthcheck directly on the PostgreSQL host — not via a remote connection.",
			fmt.Sprintf("syscall.Statfs error: %v", err),
			"https://www.postgresql.org/docs/current/storage-file-layout.html")}
	}
	bsize := uint64(stat.Bsize) //nolint:unconvert
	total := stat.Blocks * bsize
	avail := stat.Bavail * bsize
	pct := 0
	if total > 0 {
		pct = int(float64(total-avail) / float64(total) * 100)
	}
	obs := fmt.Sprintf("Data directory filesystem %d%% used (%.1f GB free of %.1f GB)",
		pct, float64(avail)/1024/1024/1024, float64(total)/1024/1024/1024)
	if pct >= 90 {
		return []Finding{NewCrit("G13-010", g13, "Data directory disk space", obs,
			"Free space immediately — PostgreSQL will crash when the data directory filesystem is full.",
			"Less than 10% disk space remaining. A full data directory filesystem causes PostgreSQL "+
				"to halt all write operations and may corrupt in-flight transactions.",
			"https://www.postgresql.org/docs/current/storage-file-layout.html")}
	}
	if pct >= 80 {
		return []Finding{NewWarn("G13-010", g13, "Data directory disk space", obs,
			"Expand the filesystem, archive old data, or move tablespaces before reaching critical levels.",
			"Less than 20% disk space remaining on the data directory filesystem.",
			"https://www.postgresql.org/docs/current/storage-file-layout.html")}
	}
	return []Finding{NewOK("G13-010", g13, "Data directory disk space", obs,
		"https://www.postgresql.org/docs/current/storage-file-layout.html")}
}

// G13-011 Postmaster uptime and recent restart detection
// A PostgreSQL restart within the last hour is a strong indicator of an unexpected
// crash (OOM kill, panic, or kernel signal). Restarts within 24 hours are noted
// as informational to prompt log review even for planned maintenance.
func g13PostmasterUptime(ctx context.Context, db *pgxpool.Pool) []Finding {
	var startTime time.Time
	if err := db.QueryRow(ctx, "SELECT pg_postmaster_start_time()").Scan(&startTime); err != nil {
		return []Finding{NewSkip("G13-011", g13, "Postmaster uptime", err.Error())}
	}
	uptime := time.Since(startTime)
	obs := fmt.Sprintf("PostgreSQL up for %s (started %s)",
		g13FormatDuration(uptime), startTime.UTC().Format("2006-01-02 15:04:05 UTC"))
	if uptime < time.Hour {
		return []Finding{NewWarn("G13-011", g13, "Postmaster uptime", obs,
			"Check PostgreSQL logs and system journal for crash cause: journalctl -u postgresql --since '-2h'",
			"PostgreSQL restarted within the last hour. This may indicate an OOM kill, kernel signal, "+
				"or unexpected crash. Review logs before dismissing.",
			"https://www.postgresql.org/docs/current/server-start.html")}
	}
	if uptime < 24*time.Hour {
		return []Finding{NewInfo("G13-011", g13, "Postmaster uptime", obs,
			"Review PostgreSQL logs to confirm this was a planned restart.",
			"PostgreSQL was restarted within the last 24 hours.",
			"https://www.postgresql.org/docs/current/server-start.html")}
	}
	return []Finding{NewOK("G13-011", g13, "Postmaster uptime", obs,
		"https://www.postgresql.org/docs/current/server-start.html")}
}

// g13FormatDuration returns a human-readable duration string.
func g13FormatDuration(d time.Duration) string {
	if d >= 24*time.Hour {
		return fmt.Sprintf("%.0f days", d.Hours()/24)
	}
	if d >= time.Hour {
		return fmt.Sprintf("%.1f hours", d.Hours())
	}
	return fmt.Sprintf("%.0f minutes", d.Minutes())
}
