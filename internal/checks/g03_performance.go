package checks

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/pg_healthcheck/internal/config"
)

const g03 = "Performance Parameters"

// G03Performance checks PostgreSQL performance configuration parameters.
type G03Performance struct{}

func (g *G03Performance) Name() string    { return g03 }
func (g *G03Performance) GroupID() string { return "G03" }

func (g *G03Performance) Run(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) ([]Finding, error) {
	var f []Finding
	// Sample pg_stat_io once (PG 16+ only) and share the result with the two
	// checks that benefit from real I/O latency data.
	io := g03SampleIO(ctx, db)
	f = append(f, g03SharedBuffers(ctx, db)...)
	f = append(f, g03WorkMem(ctx, db)...)
	f = append(f, g03MaintenanceWorkMem(ctx, db)...)
	f = append(f, g03EffectiveCacheSize(ctx, db)...)
	f = append(f, g03ParallelWorkers(ctx, db)...)
	f = append(f, g03MinParallelScanSize(ctx, db)...)
	f = append(f, g03CheckpointCompletionTarget(ctx, db)...)
	f = append(f, g03CheckpointRatio(ctx, db)...)
	f = append(f, g03WALCompression(ctx, db)...)
	f = append(f, g03RandomPageCost(ctx, db, io)...)
	f = append(f, g03EffectiveIOConcurrency(ctx, db, io)...)
	f = append(f, g03JITOverhead(ctx, db)...)
	f = append(f, g03WALBuffers(ctx, db)...)
	f = append(f, g03DefaultStatisticsTarget(ctx, db)...)
	f = append(f, g03TempFiles(ctx, db)...)
	f = append(f, g03CacheHitRatio(ctx, db)...)
	return f, nil
}

// ── pg_stat_io helper (PG 16+) ───────────────────────────────────────────────

// pgIOSample holds aggregate I/O read statistics from pg_stat_io.
// hasTimings is false on PG < 16, when track_io_timing is off, or when fewer
// than minIOSamples blocks have been read (not enough data to trust the average).
type pgIOSample struct {
	reads      int64
	avgReadMs  float64 // mean per-block read latency in milliseconds
	hasTimings bool
}

// minIOSamples is the minimum number of read calls required before we trust
// the latency average.  Below this threshold the mean is too noisy.
const minIOSamples = 1000

// ssdLatencyThresholdMs: below this per-block read latency the storage is
// considered SSD-like (NVMe ≈ 0.05 ms, SATA SSD ≈ 0.2 ms, SAN ≈ 0.5 ms,
// HDD ≈ 5–10 ms).  1 ms is a conservative cut-off that keeps HDD safe.
const ssdLatencyThresholdMs = 1.0

// g03SampleIO queries pg_stat_io on PG 16+ with track_io_timing=on.
// It returns a zero-value pgIOSample (hasTimings=false) on older servers,
// when timing is disabled, or when there are not enough samples yet.
func g03SampleIO(ctx context.Context, db *pgxpool.Pool) pgIOSample {
	var major int
	if err := db.QueryRow(ctx,
		"SELECT current_setting('server_version_num')::int/10000").Scan(&major); err != nil || major < 16 {
		return pgIOSample{}
	}
	// Sum reads and read_time across client backends for 'normal' and 'bulkread'
	// contexts.  'normal' covers index lookups and heap fetches (the random reads
	// that random_page_cost models); 'bulkread' covers sequential scans.
	// Rows where read_time=0 are excluded so we only aggregate timed rows.
	const q = `
		SELECT coalesce(sum(reads), 0), coalesce(sum(read_time), 0)
		FROM   pg_stat_io
		WHERE  reads     > 0
		  AND  read_time > 0
		  AND  context   IN ('normal', 'bulkread')`
	var totalReads int64
	var totalTimeMs float64
	if err := db.QueryRow(ctx, q).Scan(&totalReads, &totalTimeMs); err != nil ||
		totalReads < minIOSamples || totalTimeMs == 0 {
		return pgIOSample{}
	}
	return pgIOSample{
		reads:      totalReads,
		avgReadMs:  totalTimeMs / float64(totalReads),
		hasTimings: true,
	}
}

// G03-001 shared_buffers vs bgwriter eviction
func g03SharedBuffers(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT buffers_clean, buffers_alloc FROM pg_stat_bgwriter`
	var clean, alloc int64
	if err := db.QueryRow(ctx, q).Scan(&clean, &alloc); err != nil {
		return []Finding{NewSkip("G03-001", g03, "shared_buffers eviction ratio", err.Error())}
	}
	if alloc == 0 {
		return []Finding{NewOK("G03-001", g03, "shared_buffers eviction ratio",
			"No buffer allocations recorded yet",
			"https://www.postgresql.org/docs/current/runtime-config-resource.html")}
	}
	pct := int(clean * 100 / alloc)
	obs := fmt.Sprintf("bgwriter evicted %d/%d buffers (%d%%)", clean, alloc, pct)
	if pct > 10 {
		return []Finding{NewWarn("G03-001", g03, "shared_buffers eviction ratio", obs,
			"Increase shared_buffers (typically 25% of RAM).",
			"High eviction rate indicates shared_buffers is too small for the working set.",
			"https://www.postgresql.org/docs/current/runtime-config-resource.html")}
	}
	return []Finding{NewOK("G03-001", g03, "shared_buffers eviction ratio", obs,
		"https://www.postgresql.org/docs/current/runtime-config-resource.html")}
}

// G03-002 work_mem × max_connections × 4 worst-case
func g03WorkMem(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT
		(SELECT setting::bigint FROM pg_settings WHERE name='work_mem'),
		(SELECT setting::bigint FROM pg_settings WHERE name='max_connections')`
	var workMem, maxConn int64
	if err := db.QueryRow(ctx, q).Scan(&workMem, &maxConn); err != nil {
		return []Finding{NewSkip("G03-002", g03, "work_mem worst-case total", err.Error())}
	}
	// work_mem is in kB, multiply by 1024 to get bytes
	worstBytes := workMem * 1024 * maxConn * 4
	worstGB := worstBytes / (1024 * 1024 * 1024)
	obs := fmt.Sprintf("work_mem=%dkB × max_connections=%d × 4 = %dGB worst-case", workMem, maxConn, worstGB)
	if worstGB > 200 {
		return []Finding{NewWarn("G03-002", g03, "work_mem worst-case total", obs,
			"Reduce work_mem or max_connections to avoid OOM under parallel query load.",
			"Worst-case RAM is work_mem × max_connections × 4 (sort nodes per query).",
			"https://www.postgresql.org/docs/current/runtime-config-resource.html")}
	}
	return []Finding{NewOK("G03-002", g03, "work_mem worst-case total", obs,
		"https://www.postgresql.org/docs/current/runtime-config-resource.html")}
}

// G03-003 maintenance_work_mem < 256 MB
func g03MaintenanceWorkMem(ctx context.Context, db *pgxpool.Pool) []Finding {
	var mwm int64
	if err := db.QueryRow(ctx, "SELECT setting::bigint FROM pg_settings WHERE name='maintenance_work_mem'").Scan(&mwm); err != nil {
		return []Finding{NewSkip("G03-003", g03, "maintenance_work_mem", err.Error())}
	}
	mwmMB := mwm / 1024
	obs := fmt.Sprintf("maintenance_work_mem = %dMB", mwmMB)
	if mwmMB < 256 {
		return []Finding{NewInfo("G03-003", g03, "maintenance_work_mem", obs,
			"Consider setting maintenance_work_mem >= 256MB for faster VACUUM and index builds.",
			"Low maintenance_work_mem slows down autovacuum and DDL operations.",
			"https://www.postgresql.org/docs/current/runtime-config-resource.html")}
	}
	return []Finding{NewOK("G03-003", g03, "maintenance_work_mem", obs,
		"https://www.postgresql.org/docs/current/runtime-config-resource.html")}
}

// G03-004 effective_cache_size still at default 4GB
func g03EffectiveCacheSize(ctx context.Context, db *pgxpool.Pool) []Finding {
	var ecs int64
	if err := db.QueryRow(ctx, "SELECT setting::bigint FROM pg_settings WHERE name='effective_cache_size'").Scan(&ecs); err != nil {
		return []Finding{NewSkip("G03-004", g03, "effective_cache_size", err.Error())}
	}
	// default is 524288 (8kB pages × 524288 = 4GB)
	const defaultVal = 524288
	ecsMB := ecs * 8 / 1024
	obs := fmt.Sprintf("effective_cache_size = %dMB (%d pages)", ecsMB, ecs)
	if ecs == defaultVal {
		return []Finding{NewInfo("G03-004", g03, "effective_cache_size", obs,
			"Set effective_cache_size to ~75% of total RAM for better query plan decisions.",
			"The default 4GB setting causes the planner to underestimate available OS page cache.",
			"https://www.postgresql.org/docs/current/runtime-config-query.html")}
	}
	return []Finding{NewOK("G03-004", g03, "effective_cache_size", obs,
		"https://www.postgresql.org/docs/current/runtime-config-query.html")}
}

// G03-005 max_parallel_workers_per_gather <= 1 on servers with >= 4 max_worker_processes
func g03ParallelWorkers(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT
		(SELECT setting::int FROM pg_settings WHERE name='max_parallel_workers_per_gather'),
		(SELECT setting::int FROM pg_settings WHERE name='max_worker_processes')`
	var mpwpg, mwp int
	if err := db.QueryRow(ctx, q).Scan(&mpwpg, &mwp); err != nil {
		return []Finding{NewSkip("G03-005", g03, "max_parallel_workers_per_gather", err.Error())}
	}
	obs := fmt.Sprintf("max_parallel_workers_per_gather=%d, max_worker_processes=%d", mpwpg, mwp)
	if mpwpg <= 1 && mwp >= 4 {
		return []Finding{NewWarn("G03-005", g03, "max_parallel_workers_per_gather", obs,
			"Increase max_parallel_workers_per_gather to leverage available CPU cores.",
			"Parallel query is effectively disabled with <= 1 parallel worker.",
			"https://www.postgresql.org/docs/current/runtime-config-query.html")}
	}
	return []Finding{NewOK("G03-005", g03, "max_parallel_workers_per_gather", obs,
		"https://www.postgresql.org/docs/current/runtime-config-query.html")}
}

// G03-006 min_parallel_table_scan_size > 128 MB
func g03MinParallelScanSize(ctx context.Context, db *pgxpool.Pool) []Finding {
	var pages int64
	if err := db.QueryRow(ctx, "SELECT setting::bigint FROM pg_settings WHERE name='min_parallel_table_scan_size'").Scan(&pages); err != nil {
		return []Finding{NewSkip("G03-006", g03, "min_parallel_table_scan_size", err.Error())}
	}
	// value is in 8kB pages
	mb := pages * 8 / 1024
	obs := fmt.Sprintf("min_parallel_table_scan_size = %dMB", mb)
	if mb > 128 {
		return []Finding{NewWarn("G03-006", g03, "min_parallel_table_scan_size", obs,
			"Consider lowering min_parallel_table_scan_size to enable parallel scans on smaller tables.",
			"Large threshold prevents parallel scans on moderately sized tables.",
			"https://www.postgresql.org/docs/current/runtime-config-query.html")}
	}
	return []Finding{NewOK("G03-006", g03, "min_parallel_table_scan_size", obs,
		"https://www.postgresql.org/docs/current/runtime-config-query.html")}
}

// G03-007 checkpoint_completion_target < 0.9
func g03CheckpointCompletionTarget(ctx context.Context, db *pgxpool.Pool) []Finding {
	var val float64
	if err := db.QueryRow(ctx, "SELECT setting::float FROM pg_settings WHERE name='checkpoint_completion_target'").Scan(&val); err != nil {
		return []Finding{NewSkip("G03-007", g03, "checkpoint_completion_target", err.Error())}
	}
	obs := fmt.Sprintf("checkpoint_completion_target = %.2f", val)
	if val < 0.9 {
		return []Finding{NewWarn("G03-007", g03, "checkpoint_completion_target", obs,
			"Set checkpoint_completion_target=0.9 to spread checkpoint I/O evenly.",
			"Low value causes I/O spikes at checkpoint completion.",
			"https://www.postgresql.org/docs/current/runtime-config-wal.html")}
	}
	return []Finding{NewOK("G03-007", g03, "checkpoint_completion_target", obs,
		"https://www.postgresql.org/docs/current/runtime-config-wal.html")}
}

// G03-008 checkpoints_req/(checkpoints_req+checkpoints_timed) > 20%
func g03CheckpointRatio(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT checkpoints_req, checkpoints_timed FROM pg_stat_bgwriter`
	var req, timed int64
	if err := db.QueryRow(ctx, q).Scan(&req, &timed); err != nil {
		return []Finding{NewSkip("G03-008", g03, "Requested checkpoint ratio", err.Error())}
	}
	total := req + timed
	if total == 0 {
		return []Finding{NewOK("G03-008", g03, "Requested checkpoint ratio",
			"No checkpoints recorded yet",
			"https://www.postgresql.org/docs/current/runtime-config-wal.html")}
	}
	pct := int(req * 100 / total)
	obs := fmt.Sprintf("checkpoints_req=%d, checkpoints_timed=%d (%d%% requested)", req, timed, pct)
	if pct > 20 {
		return []Finding{NewWarn("G03-008", g03, "Requested checkpoint ratio", obs,
			"Increase max_wal_size or reduce WAL generation to reduce forced checkpoints.",
			"Frequent requested checkpoints indicate max_wal_size is too small.",
			"https://www.postgresql.org/docs/current/runtime-config-wal.html")}
	}
	return []Finding{NewOK("G03-008", g03, "Requested checkpoint ratio", obs,
		"https://www.postgresql.org/docs/current/runtime-config-wal.html")}
}

// G03-009 wal_compression = off
func g03WALCompression(ctx context.Context, db *pgxpool.Pool) []Finding {
	var val string
	if err := db.QueryRow(ctx, "SELECT setting FROM pg_settings WHERE name='wal_compression'").Scan(&val); err != nil {
		return []Finding{NewSkip("G03-009", g03, "wal_compression", err.Error())}
	}
	if val == "off" {
		return []Finding{NewInfo("G03-009", g03, "wal_compression",
			"wal_compression = off",
			"Enable wal_compression=on (or lz4/zstd on PG15+) to reduce WAL volume.",
			"WAL compression reduces I/O and replication bandwidth at low CPU cost.",
			"https://www.postgresql.org/docs/current/runtime-config-wal.html")}
	}
	return []Finding{NewOK("G03-009", g03, "wal_compression",
		fmt.Sprintf("wal_compression = %s", val),
		"https://www.postgresql.org/docs/current/runtime-config-wal.html")}
}

// G03-010 random_page_cost = 4.0 (HDD default)
// On PG 16+ with track_io_timing=on the actual mean block-read latency from
// pg_stat_io is used to validate or suppress the warning:
//   - latency < 1 ms  → SSD-like  → WARN (evidence included in detail)
//   - latency >= 1 ms → HDD-like  → OK   (4.0 is appropriate; suppressed)
//   - no timing data  → fall back to config-only heuristic (current behaviour)
func g03RandomPageCost(ctx context.Context, db *pgxpool.Pool, io pgIOSample) []Finding {
	var val float64
	if err := db.QueryRow(ctx, "SELECT setting::float FROM pg_settings WHERE name='random_page_cost'").Scan(&val); err != nil {
		return []Finding{NewSkip("G03-010", g03, "random_page_cost", err.Error())}
	}
	obs := fmt.Sprintf("random_page_cost = %.1f", val)

	if val < 4.0 {
		// Already tuned — nothing to check.
		return []Finding{NewOK("G03-010", g03, "random_page_cost", obs,
			"https://www.postgresql.org/docs/current/runtime-config-query.html")}
	}

	// val >= 4.0 (factory HDD default).  Use pg_stat_io latency when available.
	if io.hasTimings {
		ioEvidence := fmt.Sprintf("avg %.3f ms/block over %d reads (pg_stat_io)", io.avgReadMs, io.reads)
		if io.avgReadMs < ssdLatencyThresholdMs {
			// Measured latency is SSD-like — the 4.0 default is wrong.
			return []Finding{NewWarn("G03-010", g03, "random_page_cost", obs,
				"Set random_page_cost=1.1 for SSD storage or 2.0 for SAN/RAID.",
				fmt.Sprintf("Storage is SSD-like (%s). "+
					"random_page_cost=4.0 over-penalises index scans and may cause full-table scans instead.",
					ioEvidence),
				"https://www.postgresql.org/docs/current/runtime-config-query.html")}
		}
		// Measured latency is HDD-like — 4.0 is appropriate; suppress the warning.
		return []Finding{NewOK("G03-010", g03, "random_page_cost",
			fmt.Sprintf("%s — storage latency confirms HDD-class I/O (%s)", obs, ioEvidence),
			"https://www.postgresql.org/docs/current/runtime-config-query.html")}
	}

	// No pg_stat_io data (PG < 16 or track_io_timing=off) — fall back to
	// config-only heuristic and note how to get storage-aware detection.
	return []Finding{NewWarn("G03-010", g03, "random_page_cost", obs,
		"Set random_page_cost=1.1 for SSD storage or 2.0 for SAN/RAID.",
		"The default 4.0 is tuned for spinning disks; SSDs have near-sequential random I/O. "+
			"Enable track_io_timing=on (PG 16+) for storage-aware detection.",
		"https://www.postgresql.org/docs/current/runtime-config-query.html")}
}

// G03-011 effective_io_concurrency <= 1
// On PG 16+ with track_io_timing=on the actual mean block-read latency from
// pg_stat_io is used to validate or suppress the warning:
//   - latency < 1 ms  → SSD-like  → WARN (evidence included in detail)
//   - latency >= 1 ms → HDD-like  → OK   (low concurrency suits spinning disks)
//   - no timing data  → fall back to config-only heuristic (current behaviour)
func g03EffectiveIOConcurrency(ctx context.Context, db *pgxpool.Pool, io pgIOSample) []Finding {
	var val int
	if err := db.QueryRow(ctx, "SELECT setting::int FROM pg_settings WHERE name='effective_io_concurrency'").Scan(&val); err != nil {
		return []Finding{NewSkip("G03-011", g03, "effective_io_concurrency", err.Error())}
	}
	obs := fmt.Sprintf("effective_io_concurrency = %d", val)

	if val > 1 {
		// Already tuned — nothing to check.
		return []Finding{NewOK("G03-011", g03, "effective_io_concurrency", obs,
			"https://www.postgresql.org/docs/current/runtime-config-resource.html")}
	}

	// val <= 1.  Use pg_stat_io latency when available.
	if io.hasTimings {
		ioEvidence := fmt.Sprintf("avg %.3f ms/block over %d reads (pg_stat_io)", io.avgReadMs, io.reads)
		if io.avgReadMs < ssdLatencyThresholdMs {
			// SSD-like — increasing concurrency will let bitmap heap scans prefetch more pages.
			return []Finding{NewWarn("G03-011", g03, "effective_io_concurrency", obs,
				"Set effective_io_concurrency=200 for SSD or 2-4 for RAID.",
				fmt.Sprintf("Storage is SSD-like (%s). "+
					"Low concurrency prevents bitmap heap scans from issuing parallel prefetch requests.",
					ioEvidence),
				"https://www.postgresql.org/docs/current/runtime-config-resource.html")}
		}
		// HDD-like — low concurrency is appropriate; suppress the warning.
		return []Finding{NewOK("G03-011", g03, "effective_io_concurrency",
			fmt.Sprintf("%s — storage latency confirms HDD-class I/O (%s)", obs, ioEvidence),
			"https://www.postgresql.org/docs/current/runtime-config-resource.html")}
	}

	// No pg_stat_io data — fall back to config-only heuristic.
	return []Finding{NewWarn("G03-011", g03, "effective_io_concurrency", obs,
		"Set effective_io_concurrency=200 for SSD or 2-4 for RAID.",
		"Low concurrency prevents bitmap heap scans from prefetching pages. "+
			"Enable track_io_timing=on (PG 16+) for storage-aware detection.",
		"https://www.postgresql.org/docs/current/runtime-config-resource.html")}
}

// G03-012 JIT overhead from pg_stat_statements
func g03JITOverhead(ctx context.Context, db *pgxpool.Pool) []Finding {
	var jitOn string
	if err := db.QueryRow(ctx, "SELECT setting FROM pg_settings WHERE name='jit'").Scan(&jitOn); err != nil {
		return []Finding{NewSkip("G03-012", g03, "JIT overhead", err.Error())}
	}
	if jitOn != "on" {
		return []Finding{NewOK("G03-012", g03, "JIT overhead", "jit = off",
			"https://www.postgresql.org/docs/current/jit.html")}
	}
	const q = `SELECT coalesce(sum(jit_functions + jit_optimization_count + jit_emission_count), 0)
		FROM pg_stat_statements`
	var total int64
	if err := db.QueryRow(ctx, q).Scan(&total); err != nil {
		return []Finding{NewSkip("G03-012", g03, "JIT overhead",
			"pg_stat_statements not available: "+err.Error())}
	}
	obs := fmt.Sprintf("jit=on; total JIT compilations: %d", total)
	if total > 0 {
		return []Finding{NewInfo("G03-012", g03, "JIT overhead", obs,
			"Review jit_above_cost and jit_inline_above_cost thresholds if JIT is causing latency spikes.",
			"JIT can improve throughput for analytical queries but adds latency for OLTP workloads.",
			"https://www.postgresql.org/docs/current/jit.html")}
	}
	return []Finding{NewOK("G03-012", g03, "JIT overhead", obs,
		"https://www.postgresql.org/docs/current/jit.html")}
}

// G03-013 wal_buffers < 1MB
func g03WALBuffers(ctx context.Context, db *pgxpool.Pool) []Finding {
	var val int64
	if err := db.QueryRow(ctx, "SELECT setting::bigint FROM pg_settings WHERE name='wal_buffers'").Scan(&val); err != nil {
		return []Finding{NewSkip("G03-013", g03, "wal_buffers", err.Error())}
	}
	// value is in 8kB pages; -1 means auto
	if val == -1 {
		return []Finding{NewOK("G03-013", g03, "wal_buffers",
			"wal_buffers = -1 (auto)",
			"https://www.postgresql.org/docs/current/runtime-config-wal.html")}
	}
	kb := val * 8
	obs := fmt.Sprintf("wal_buffers = %dkB (%d pages)", kb, val)
	if kb < 1024 {
		return []Finding{NewInfo("G03-013", g03, "wal_buffers", obs,
			"Set wal_buffers=16MB or -1 (auto) for write-heavy workloads.",
			"Small wal_buffers increases lock contention on the WAL write lock.",
			"https://www.postgresql.org/docs/current/runtime-config-wal.html")}
	}
	return []Finding{NewOK("G03-013", g03, "wal_buffers", obs,
		"https://www.postgresql.org/docs/current/runtime-config-wal.html")}
}

// G03-014 default_statistics_target = 100 (default)
func g03DefaultStatisticsTarget(ctx context.Context, db *pgxpool.Pool) []Finding {
	var val int
	if err := db.QueryRow(ctx, "SELECT setting::int FROM pg_settings WHERE name='default_statistics_target'").Scan(&val); err != nil {
		return []Finding{NewSkip("G03-014", g03, "default_statistics_target", err.Error())}
	}
	obs := fmt.Sprintf("default_statistics_target = %d", val)
	if val == 100 {
		return []Finding{NewInfo("G03-014", g03, "default_statistics_target", obs,
			"Consider increasing default_statistics_target to 200-500 for complex queries on large tables.",
			"The default 100 may produce suboptimal plans for tables with non-uniform data distribution.",
			"https://www.postgresql.org/docs/current/runtime-config-query.html")}
	}
	return []Finding{NewOK("G03-014", g03, "default_statistics_target", obs,
		"https://www.postgresql.org/docs/current/runtime-config-query.html")}
}

// G03-016 database cache hit ratio from pg_stat_database
// Uses blks_hit and blks_read from pg_stat_database for a cluster-wide view.
// A cache hit ratio below 95% on an OLTP system usually means shared_buffers
// is too small or the working set exceeds available memory.
func g03CacheHitRatio(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT sum(blks_hit), sum(blks_read) FROM pg_stat_database`
	var hits, reads int64
	if err := db.QueryRow(ctx, q).Scan(&hits, &reads); err != nil {
		return []Finding{NewSkip("G03-016", g03, "Database cache hit ratio", err.Error())}
	}
	total := hits + reads
	if total == 0 {
		return []Finding{NewOK("G03-016", g03, "Database cache hit ratio",
			"No I/O data yet",
			"https://www.postgresql.org/docs/current/monitoring-stats.html")}
	}
	pct := hits * 100 / total
	obs := fmt.Sprintf("Cache hit ratio: %d%%  (%d hits / %d total accesses)", pct, hits, total)
	if pct < 90 {
		return []Finding{NewWarn("G03-016", g03, "Database cache hit ratio", obs,
			"Increase shared_buffers (typically 25% of RAM) to reduce disk I/O.",
			"Cache hit ratio below 90% means frequent disk reads which degrade query performance.",
			"https://www.postgresql.org/docs/current/runtime-config-resource.html")}
	}
	if pct < 95 {
		return []Finding{NewInfo("G03-016", g03, "Database cache hit ratio", obs,
			"Consider increasing shared_buffers if the working set fits in RAM.",
			"Ideal cache hit ratio for OLTP workloads is 95%+.",
			"https://www.postgresql.org/docs/current/runtime-config-resource.html")}
	}
	return []Finding{NewOK("G03-016", g03, "Database cache hit ratio", obs,
		"https://www.postgresql.org/docs/current/runtime-config-resource.html")}
}

// G03-015 SUM(temp_files) > 1000 or SUM(temp_bytes) > 10 GB
func g03TempFiles(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT coalesce(sum(temp_files),0), coalesce(sum(temp_bytes),0) FROM pg_stat_database`
	var files int64
	var bytes int64
	if err := db.QueryRow(ctx, q).Scan(&files, &bytes); err != nil {
		return []Finding{NewSkip("G03-015", g03, "Temp file spill", err.Error())}
	}
	const tenGB = int64(10 * 1024 * 1024 * 1024)
	obs := fmt.Sprintf("Total temp files: %d, temp bytes: %dMB", files, bytes/1024/1024)
	var msgs []string
	if files > 1000 {
		msgs = append(msgs, fmt.Sprintf("%d temp files (threshold: 1000)", files))
	}
	if bytes > tenGB {
		msgs = append(msgs, fmt.Sprintf("%dGB temp spill (threshold: 10GB)", bytes/1024/1024/1024))
	}
	if len(msgs) > 0 {
		return []Finding{NewWarn("G03-015", g03, "Temp file spill", obs,
			"Increase work_mem to reduce temp file spill.",
			strings.Join(msgs, "; "),
			"https://www.postgresql.org/docs/current/runtime-config-resource.html")}
	}
	return []Finding{NewOK("G03-015", g03, "Temp file spill", obs,
		"https://www.postgresql.org/docs/current/runtime-config-resource.html")}
}
