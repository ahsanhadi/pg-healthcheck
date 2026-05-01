"""
Generate pg_healthcheck Check Reference PDF
One line per check across all 15 groups.
"""

from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, KeepTogether
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from datetime import date

OUTPUT = "/Users/ahsanhadi/pg healthcheck/pg_healthcheck_check_reference.pdf"

# ── Palette ────────────────────────────────────────────────────────────────
DARK_BLUE   = colors.HexColor("#1B3A5C")
MID_BLUE    = colors.HexColor("#2E6DA4")
LIGHT_BLUE  = colors.HexColor("#EBF3FB")
STRIPE      = colors.HexColor("#F5F8FC")
WHITE       = colors.white
RED         = colors.HexColor("#C0392B")
ORANGE      = colors.HexColor("#E67E22")
GREEN       = colors.HexColor("#27AE60")
GREY_TEXT   = colors.HexColor("#555555")
LIGHT_GREY  = colors.HexColor("#CCCCCC")

# ── Check data ─────────────────────────────────────────────────────────────
# (check_id, one-line description)
CHECKS = [
    # G01 — Connection & Availability
    ("G01-001", "Verifies the PostgreSQL TCP port is reachable and measures connection time in milliseconds"),
    ("G01-002", "Measures the pg_isready round-trip time; warns when response exceeds the configured threshold"),
    ("G01-003", "Confirms ssl=on is set in postgresql.conf; warns when SSL is disabled"),
    ("G01-004", "Connects via TLS and reads the server certificate expiry date; warns/criticals before expiry"),
    ("G01-005", "Checks the major PostgreSQL version against the official end-of-life calendar"),
    ("G01-006", "Compares active client backend connections against max_connections minus superuser_reserved_connections"),
    ("G01-007", "Lists sessions idle in transaction longer than the configured threshold — these hold locks"),
    ("G01-008", "Checks per-database connection counts; warns when any single database exceeds 50% of max_connections"),
    ("G01-009", "Scans pg_hba_file_rules for TRUST auth on non-loopback addresses — a critical security misconfiguration"),

    # G02 — pgBackRest Backup
    ("G02-000", "Detects whether pgBackRest is installed; emits a single INFO skip if not present (all G02 checks skipped)"),
    ("G02-001", "Checks archive-async=y in pgbackrest.conf; synchronous archiving blocks WAL writes"),
    ("G02-002", "Verifies spool-path is set and the directory exists when archive-async is enabled"),
    ("G02-003", "Compares process-max against available CPU cores; too few workers bottleneck parallel backup"),
    ("G02-004", "Checks compress-type is set to a modern algorithm (lz4/zstd) rather than gz"),
    ("G02-005", "Validates compress-level is within the recommended range for the chosen algorithm"),
    ("G02-006", "Checks buffer-size is set to at least 4 MB for efficient network transfers"),
    ("G02-007", "Verifies backup-standby=y is configured on multi-node clusters to offload backup I/O"),
    ("G02-008", "Queries pg_settings to confirm archive_command is set and not empty"),
    ("G02-009", "Counts .ready WAL files in the pg_wal/archive_status directory; high count means archiving is falling behind"),
    ("G02-010", "Checks the age of the oldest .ready file; stale files indicate the archiver is stuck"),
    ("G02-011", "Reads pg_stat_archiver for failed_count and last_failed_time since the last reset"),
    ("G02-012", "Runs pgbackrest info --output=json to check when the last successful backup completed"),
    ("G02-013", "Reads repo-retention-full from pgbackrest.conf and warns when fewer than the minimum full backups exist"),
    ("G02-014", "Reports archive-push-queue-max setting as an informational advisory"),

    # G03 — Performance Parameters
    ("G03-001", "Compares bgwriter buffers_clean against buffers_alloc; high ratio means shared_buffers is too small"),
    ("G03-002", "Calculates worst-case memory = work_mem x max_connections x 4; warns when it exceeds available RAM estimate"),
    ("G03-003", "Warns when maintenance_work_mem is below 256 MB — low values slow VACUUM and index builds"),
    ("G03-004", "Flags effective_cache_size still at the PostgreSQL default of 4 GB, which underestimates modern systems"),
    ("G03-005", "Warns when max_parallel_workers_per_gather is 1 or less on servers with 4+ CPU worker processes"),
    ("G03-006", "Flags min_parallel_table_scan_size above 128 MB, which prevents parallelism on medium-sized tables"),
    ("G03-007", "Warns when checkpoint_completion_target is below 0.9 — low values cause I/O spikes at checkpoint end"),
    ("G03-008", "Checks the ratio of forced (checkpoints_req) to scheduled checkpoints; >20% means max_wal_size is too small"),
    ("G03-009", "Warns when wal_compression is off; enabling it reduces WAL volume with minimal CPU overhead"),
    ("G03-010", "Flags random_page_cost=4.0 (spinning disk default) on SSD storage using pg_stat_io latency on PG 16+"),
    ("G03-011", "Warns when effective_io_concurrency is 1 on SSD storage — low value prevents bitmap scan prefetching"),
    ("G03-012", "Checks pg_stat_statements for queries with very high total execution time indicating JIT overhead"),
    ("G03-013", "Warns when wal_buffers is below 1 MB — the default -1 auto-sizes but may be too small on busy systems"),
    ("G03-014", "Notes when default_statistics_target is still at 100 — increasing it improves plans on large irregular tables"),
    ("G03-015", "Sums temp_files and temp_bytes across all databases; high values indicate work_mem is too small"),
    ("G03-016", "Calculates cluster-wide cache hit ratio from pg_stat_database; warns below 90%, notes below 95%"),
    ("G03-017", "Warns when track_io_timing=off — disables per-block I/O latency in pg_stat_io and pg_stat_statements"),

    # G04 — Locks & Blocking
    ("G04-001", "Lists queries running longer than the configured warn/critical thresholds from pg_stat_activity"),
    ("G04-002", "Detects sessions idle in transaction using pg_stat_activity; these accumulate locks silently"),
    ("G04-003", "Uses pg_blocking_pids() to identify lock blocker chains and the queries holding them"),
    ("G04-004", "Sums deadlock counts from pg_stat_database across all databases"),
    ("G04-005", "Notes when statement_timeout=0 (disabled) — unbounded statements can hold locks indefinitely"),
    ("G04-006", "Warns when idle_in_transaction_session_timeout=0 — stale transactions block autovacuum and VACUUM"),
    ("G04-007", "Checks whether pg_stat_statements is installed and loaded via shared_preload_libraries"),
    ("G04-008", "Reads pg_stat_statements for the top 10 queries by cumulative execution time"),
    ("G04-009", "Checks log_min_duration_statement; -1 means slow queries are never logged"),
    ("G04-010", "Warns when lock_timeout=0 — unbounded lock waits can cause connection pile-ups under contention"),
    ("G04-011", "Counts normalized query patterns from pg_stat_statements whose mean execution time exceeds the configured threshold with ≥5 executions — a regression signal distinct from top-N cumulative time"),

    # G05 — Vacuum & Bloat
    ("G05-001", "Reads age(datfrozenxid) for every database; warns/criticals based on TXID wraparound thresholds"),
    ("G05-002", "Reads age(relfrozenxid) for every table; identifies individual tables approaching wraparound"),
    ("G05-003", "Flags tables where dead tuples exceed 20% of live tuples and live tuple count is above 10,000"),
    ("G05-004", "Finds high-write tables whose last_autovacuum is older than 7 days"),
    ("G05-005", "Detects tables where autovacuum has been explicitly disabled via storage parameters"),
    ("G05-006", "Warns when autovacuum_vacuum_scale_factor >= 0.2 — too high for large tables (defers vacuum too long)"),
    ("G05-007", "Compares autovacuum_max_workers against the number of high-write tables in the database"),
    ("G05-008", "Warns when autovacuum_vacuum_cost_delay exceeds 10 ms — high delay throttles autovacuum excessively"),
    ("G05-009", "Estimates table bloat from pg_stat_user_tables; excludes tables smaller than 1 MB to avoid noise"),
    ("G05-010", "Checks pg_stat_progress_vacuum for VACUUM processes running longer than 4 hours"),
    ("G05-011", "Warns when autovacuum_work_mem=-1 and maintenance_work_mem exceeds 1 GB — may over-allocate per worker"),
    ("G05-012", "Reads mxid_age(datminmxid) for all databases to detect multixact wraparound risk"),
    ("G05-013", "Warns on prepared transactions older than 1 hour; these block VACUUM from advancing the horizon"),
    ("G05-014", "Compares active autovacuum workers against autovacuum_max_workers to detect saturation"),
    ("G05-015", "Finds high-write tables whose last_autoanalyze is older than 7 days — stale stats cause bad plans"),

    # G06 — Indexes
    ("G06-001", "Finds non-unique indexes with idx_scan=0 and size > 1 MB — likely unused and wasting space"),
    ("G06-002", "Detects indexes where two or more indexes cover identical column sets"),
    ("G06-003", "Queries pg_index for indisvalid=false — invalid indexes are ignored by the planner but still updated"),
    ("G06-004", "Heuristic bloat check: flags indexes larger than 100 MB where size > n_live_tup x 250 bytes"),
    ("G06-005", "Finds foreign key columns with no supporting index — FK lookups without an index cause seq scans"),
    ("G06-006", "Detects indexes whose leading columns are a prefix of another index on the same table"),
    ("G06-007", "Finds B-tree indexes on columns with very low cardinality — bitmap scans are usually better"),
    ("G06-008", "Checks BRIN index physical correlation against the indexed column; low correlation makes BRIN useless"),
    ("G06-009", "Reports the pg_stat_bgwriter stats_reset timestamp as context for interpreting idx_scan=0 unused-index results"),
    ("G06-010", "Lists user tables with no primary key — required for logical replication and Spock cluster membership"),

    # G07 — TOAST & Data Integrity
    ("G07-001", "Queries pg_settings for data_checksums; warns if off — checksums catch silent data corruption"),
    ("G07-002", "Sums checksum_failures from pg_stat_database; any non-zero count indicates possible corruption"),
    ("G07-003", "Validates that every reltoastrelid in pg_class points to a real pg_class entry"),
    ("G07-005", "Finds TOAST tables in pg_class that have no corresponding main table (orphaned after DROP)"),
    ("G07-006", "Flags tables where the TOAST segment is more than twice the size of the main heap"),
    ("G07-007", "Runs amcheck bt_index_check() on configured tables to verify B-tree structural integrity"),
    ("G07-008", "Calculates TOAST-specific cache hit ratio from pg_statio_user_tables"),
    ("G07-009", "Probes for pg_check_relation (PG17+) or verify_heapam via amcheck (PG13-16) for heap corruption detection"),

    # G08 — Visibility Map
    ("G08-001", "Identifies tables with high heap_blks_read relative to index scans — suggests missing indexes or low fill"),
    ("G08-002", "Checks relallvisible <= relpages for all tables; inconsistency means the visibility map is stale"),
    ("G08-003", "After an unclean shutdown, advises running VACUUM ANALYZE to ensure visibility map accuracy"),
    ("G08-004", "Checks whether the pg_visibility extension is installed for deeper visibility map diagnostics"),
    ("G08-005", "Flags tables where n_dead_tup is suspiciously low despite high n_tup_upd — may indicate tracking issues"),

    # G09 — WAL & Replication Slots
    ("G09-001", "Finds inactive replication slots with retained WAL exceeding the configured warning threshold"),
    ("G09-002", "Checks retained WAL per slot in GB against configured warn/critical thresholds"),
    ("G09-003", "Warns when active slot count exceeds 80% of max_replication_slots"),
    ("G09-004", "Reads pg_stat_replication and measures replication lag in bytes per standby"),
    ("G09-005", "Flags standbys where application_name is empty or a generic default — hard to identify in monitoring"),
    ("G09-006", "Warns when recovery_min_apply_delay > 0 on a primary — usually set by accident"),
    ("G09-007", "Warns when wal_keep_size=0 and physical standbys exist without replication slots"),
    ("G09-008", "Cross-references WAL archiving (G02) and slot retention (G09) for a combined advisory message"),
    ("G09-009", "Detects invalidated replication slots; invalid slots block WAL recycling until dropped"),
    ("G09-010", "Warns when max_slot_wal_keep_size=-1 (unlimited) — a stale slot can fill the disk before PostgreSQL crashes"),
    ("G09-011", "Finds logical replication slots with no active consumer — they silently accumulate WAL"),
    ("G09-012", "Checks pg_stat_subscription for workers not running on active subscriptions"),
    ("G09-013", "Reads pg_subscription_rel for tables stuck in 'error' or non-ready sync state"),
    ("G09-014", "Measures time-based replication lag using write_lag/flush_lag/replay_lag from pg_stat_replication"),

    # G10 — Upgrade Readiness
    ("G10-000", "Gate check: if --target-version is not configured, all G10 upgrade-readiness checks are skipped with a single advisory message"),
    ("G10-001", "Checks for tsearch2 and plpython2u extensions which are removed in PostgreSQL 14+"),
    ("G10-002", "Reads PostGIS version and notes any version-specific upgrade considerations"),
    ("G10-003", "Finds columns using deprecated abstime, reltime, or tinterval types removed in PG12+"),
    ("G10-004", "Lists columns using the money type, which has locale-dependent behaviour across versions"),
    ("G10-005", "Detects SQL_ASCII databases, which cause encoding conversion failures after upgrade"),
    ("G10-006", "Records the collation version for each database; mismatches after upgrade require REFRESH"),
    ("G10-007", "Finds tables with more than 1,600 columns — the hard limit that causes pg_upgrade to fail"),
    ("G10-008", "Warns on any pending prepared transactions that must be committed or rolled back before upgrade"),
    ("G10-009", "Flags logical replication slots on PG < 17, which are not preserved by pg_upgrade"),
    ("G10-010", "Lists user-defined C-language functions that must be recompiled for the new major version"),
    ("G10-011", "Warns when plpython2u functions exist and the target version is PG14+ (Python 2 removed)"),
    ("G10-012", "Finds ghost extensions: entries in pg_extension with no corresponding files on disk"),
    ("G10-013", "Counts large objects; very high counts significantly increase pg_upgrade run time"),
    ("G10-014", "Lists custom tablespace paths that must be manually re-created on the target system"),
    ("G10-015", "Flags databases with datconnlimit=0 which blocks all new connections"),

    # G11 — Security
    ("G11-001", "Finds superuser login roles with no password set — anyone can connect without credentials"),
    ("G11-002", "Scans pg_hba_file_rules for md5 auth; scram-sha-256 is required for modern security standards"),
    ("G11-003", "Checks whether PUBLIC still has CREATE privilege on the public schema (revoked by default in PG15+)"),
    ("G11-004", "Lists non-superuser roles with CREATEROLE or CREATEDB — these can escalate privileges"),
    ("G11-005", "Warns when both log_connections and log_disconnections are off — no audit trail for connections"),
    ("G11-006", "Checks whether the pgaudit extension is installed for DDL/DML audit logging"),
    ("G11-007", "Advisory check on non-superuser login roles — PostgreSQL does not natively track last login time"),
    ("G11-008", "Reports the configured ssl_cert_file and ssl_key_file paths for manual permission verification"),
    ("G11-009", "Counts superuser login roles; more than 2 is a security risk as superusers bypass all access controls"),
    ("G11-010", "Checks password_encryption setting; md5 is deprecated and scram-sha-256 should be used"),
    ("G11-011", "Counts client connections using unencrypted (non-SSL) transport from pg_stat_ssl; warns when any exist on non-loopback addresses"),

    # G12 — pgEdge / Spock Cluster
    ("G12-000", "Gate check: if Spock is not installed on any node, all G12 checks are skipped with a single clear message"),
    ("G12-001", "Cross-node: verifies that spock.node lists the same nodes on all cluster members"),
    ("G12-002", "Checks spock.subscription for any subscriptions where sub_enabled=false"),
    ("G12-003", "Reads spock worker processes from pg_stat_activity; warns when expected workers are missing"),
    ("G12-004", "Measures apply lag per subscription from spock apply worker state"),
    ("G12-005", "Counts rows in spock.exception_log; a non-empty log means replication conflicts occurred"),
    ("G12-006", "Counts rows in spock.resolutions; a very high count means ongoing conflict pattern needs review"),
    ("G12-007", "Flags exceptions in spock.exception_log that are older than the configured age threshold"),
    ("G12-008", "Checks whether the pg_cron extension is installed — recommended for automating exception log cleanup"),
    ("G12-009", "Finds spock replication slots that are inactive — these accumulate WAL and can fill the disk"),
    ("G12-010", "Warns when hot_standby_feedback=off on Spock nodes — required to prevent standby query cancellations"),
    ("G12-011", "Verifies wal_level=logical on all Spock nodes — lower wal_level disables Spock replication"),
    ("G12-012", "Cross-node: compares the set of replicated tables across all nodes to detect schema divergence"),
    ("G12-013", "Cross-node: compares indexes on replicated tables across nodes to detect index divergence"),
    ("G12-014", "Cross-node: checks sequence increment values to detect collisions in a multi-master cluster"),
    ("G12-015", "Checks forward_origins on subscriptions; empty value in multi-master topology causes replication gaps"),
    ("G12-016", "Reports the number of tables in each replication set and warns on empty sets"),
    ("G12-017", "Reads spock sync state for all objects; flags any stuck in error or non-ready state"),
    ("G12-018", "Cross-node: samples row counts on configured tables and warns when nodes diverge beyond threshold"),
    ("G12-019", "Verifies spock.local_node is registered; missing registration means this node is not part of the cluster"),
    ("G12-020", "Reads spock.lag_tracker for per-receiver replication lag snapshot across the cluster"),
    ("G12-021", "Counts messages in spock.queue and warns when old messages are accumulating unprocessed"),
    ("G12-022", "Reads spock.channel_summary_stats for per-subscription conflict and DCA counters"),
    ("G12-023", "Reads spock.progress to report replication LSN position per node pair in MB"),

    # G13 — OS & Resource-Level
    ("G13-001", "Calculates average checkpoint_sync_time per checkpoint; >1000 ms indicates slow storage at flush time"),
    ("G13-002", "Reads pg_stat_io evictions for client backends on PG16+; high count means shared_buffers is too small"),
    ("G13-003", "Checks maxwritten_clean in pg_stat_bgwriter; >0 means bgwriter is stopping cleaning passes mid-scan"),
    ("G13-004", "Warns when huge_pages is not set to 'on' — huge pages reduce TLB pressure on large shared_buffers"),
    ("G13-005", "Sums temp_files and temp_bytes across all databases; high values indicate work_mem is too small"),
    ("G13-006", "Sums query conflicts from pg_stat_database; conflicts cancel standby queries when primary reclaims space"),
    ("G13-007", "Notes when max_connections > 200 and recommends connection pooling to reduce per-connection memory"),
    ("G13-008", "Reads /sys/kernel/mm/transparent_hugepage/enabled; warns when THP=always causes latency spikes (Linux only)"),
    ("G13-009", "Reads /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor; warns on powersave/schedutil (Linux only)"),
    ("G13-010", "Runs syscall.Statfs on the data_directory path; warns at 80% used, critical at 90% (local execution only)"),
    ("G13-011", "Queries pg_postmaster_start_time(); warns if restarted within 1 hour (possible crash), info within 24 hours"),

    # G15 — Replication Health
    ("G15-001", "On standbys, checks pg_stat_wal_receiver for an active WAL receiver process and warns if last message from primary is >5 minutes old"),
    ("G15-002", "On primaries, finds inactive physical replication slots where the standby has disconnected and WAL is accumulating"),
    ("G15-003", "On standbys, breaks down pg_stat_database_conflicts by type (snapshot, lock, bufferpin, deadlock, logicalslot) with root-cause guidance"),

    # G14 — WAL Growth & Generation Rate
    ("G14-001", "Reads pg_ls_waldir() to get total size of pg_wal directory and compares against configured thresholds"),
    ("G14-002", "Takes two LSN samples bracketing other checks and calculates WAL generation rate in MB/s"),
    ("G14-003", "Compares current WAL rate against a rolling baseline stored in a state file; warns on sudden spikes"),
    ("G14-004", "Reads pg_stat_wal (PG14+) for total WAL volume, record count, FPI count, and buffer-full events"),
    ("G14-005", "Calculates the ratio of full-page write records to total WAL records; high ratio wastes WAL space"),
    ("G14-006", "Reads pg_stat_user_tables to find the top 5 tables by n_tup_ins+n_tup_upd+n_tup_del modification count"),
    ("G14-007", "Advisory check on wal_compression=off; enabling it compresses FPI images with minimal CPU overhead"),
    ("G14-008", "Warns when wal_level=logical but no logical replication slots or subscriptions exist on this node"),
    ("G14-009", "Counts WAL segment files in pg_ls_waldir(); high count suggests recycling is blocked"),
    ("G14-010", "Reads pg_stat_archiver to check whether archiving is active and when the last successful archive occurred"),
    ("G14-011", "Lists UNLOGGED tables; converting them to logged tables causes a WAL spike for the initial sync"),
    ("G14-012", "Calculates checkpoints_req / total_checkpoints; >20% forced means max_wal_size is too small for the workload"),
    ("G14-013", "Runs syscall.Statfs on the pg_wal filesystem; warns at 60% used, critical at 80% (local execution only)"),
    ("G14-014", "Finds open transactions older than 5 minutes that are preventing WAL segment recycling"),
    ("G14-015", "Measures WAL bytes accumulated since last checkpoint vs max_wal_size; detects reserved-segment pile-up on write-heavy Spock nodes"),
]

GROUPS = [
    ("G01", "Connection & Availability"),
    ("G02", "pgBackRest Backup"),
    ("G03", "Performance Parameters"),
    ("G04", "Locks & Blocking"),
    ("G05", "Vacuum & Bloat"),
    ("G06", "Index Health"),
    ("G07", "TOAST & Data Integrity"),
    ("G08", "Visibility Map"),
    ("G09", "WAL & Replication Slots"),
    ("G10", "Upgrade Readiness"),
    ("G11", "Security Posture"),
    ("G12", "pgEdge / Spock Cluster"),
    ("G13", "OS & Resource-Level"),
    ("G14", "WAL Growth & Generation Rate"),
    ("G15", "Replication Health"),
]

def build_pdf():
    doc = SimpleDocTemplate(
        OUTPUT,
        pagesize=A4,
        leftMargin=18*mm,
        rightMargin=18*mm,
        topMargin=20*mm,
        bottomMargin=20*mm,
        title="pg_healthcheck — Check Reference",
        author="pg_healthcheck",
        subject="All checks, one line each",
    )

    styles = getSampleStyleSheet()

    title_style = ParagraphStyle("DocTitle",
        fontSize=22, leading=28, textColor=DARK_BLUE,
        fontName="Helvetica-Bold", spaceAfter=2*mm, alignment=TA_LEFT)

    subtitle_style = ParagraphStyle("DocSub",
        fontSize=10, leading=14, textColor=GREY_TEXT,
        fontName="Helvetica", spaceAfter=6*mm, alignment=TA_LEFT)

    group_style = ParagraphStyle("GroupHeader",
        fontSize=11, leading=14, textColor=WHITE,
        fontName="Helvetica-Bold", leftIndent=3*mm)

    id_style = ParagraphStyle("CheckID",
        fontSize=8, leading=10, textColor=MID_BLUE,
        fontName="Helvetica-Bold")

    desc_style = ParagraphStyle("CheckDesc",
        fontSize=8, leading=11, textColor=colors.HexColor("#222222"),
        fontName="Helvetica")

    footer_style = ParagraphStyle("Footer",
        fontSize=7, textColor=GREY_TEXT,
        fontName="Helvetica", alignment=TA_CENTER)

    story = []

    # ── Cover block ────────────────────────────────────────────────────────
    story.append(Spacer(1, 4*mm))
    story.append(Paragraph("pg_healthcheck", title_style))
    story.append(Paragraph(
        f"Check Reference &nbsp;·&nbsp; {len(CHECKS)} checks across {len(GROUPS)} groups &nbsp;·&nbsp; {date.today().strftime('%d %B %Y')}",
        subtitle_style))
    story.append(HRFlowable(width="100%", thickness=1.5, color=MID_BLUE, spaceAfter=5*mm))

    # ── Legend ────────────────────────────────────────────────────────────
    legend_data = [
        [Paragraph("<b>CHECK ID</b>", id_style),
         Paragraph("<b>WHAT IT CHECKS — one-line description</b>", desc_style)]
    ]
    legend_table = Table(legend_data, colWidths=[28*mm, 144*mm])
    legend_table.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), LIGHT_BLUE),
        ("TOPPADDING",    (0,0), (-1,-1), 3),
        ("BOTTOMPADDING", (0,0), (-1,-1), 3),
        ("LEFTPADDING",   (0,0), (-1,-1), 4),
        ("RIGHTPADDING",  (0,0), (-1,-1), 4),
        ("BOX", (0,0), (-1,-1), 0.5, LIGHT_GREY),
    ]))
    story.append(legend_table)
    story.append(Spacer(1, 4*mm))

    # ── One section per group ──────────────────────────────────────────────
    check_map = {}
    for cid, desc in CHECKS:
        prefix = cid[:3]
        check_map.setdefault(prefix, []).append((cid, desc))

    for g_prefix, g_name in GROUPS:
        group_checks = check_map.get(g_prefix, [])

        # Group header row
        header_data = [[
            Paragraph(f"{g_prefix} &nbsp; {g_name}", group_style),
            Paragraph(f"<b>{len(group_checks)} checks</b>",
                      ParagraphStyle("cnt", fontSize=9, fontName="Helvetica-Bold",
                                     textColor=WHITE, alignment=TA_CENTER))
        ]]
        header_table = Table(header_data, colWidths=[148*mm, 24*mm])
        header_table.setStyle(TableStyle([
            ("BACKGROUND",    (0,0), (-1,-1), DARK_BLUE),
            ("TOPPADDING",    (0,0), (-1,-1), 4),
            ("BOTTOMPADDING", (0,0), (-1,-1), 4),
            ("LEFTPADDING",   (0,0), (0,-1),  4),
            ("RIGHTPADDING",  (-1,0), (-1,-1), 4),
            ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
        ]))

        # Check rows
        rows = []
        for i, (cid, desc) in enumerate(group_checks):
            bg = STRIPE if i % 2 == 0 else WHITE
            rows.append([
                Paragraph(cid, id_style),
                Paragraph(desc, desc_style),
                bg
            ])

        check_data  = [[r[0], r[1]] for r in rows]
        row_colours = [r[2] for r in rows]

        checks_table = Table(check_data, colWidths=[28*mm, 144*mm])
        ts = TableStyle([
            ("TOPPADDING",    (0,0), (-1,-1), 3),
            ("BOTTOMPADDING", (0,0), (-1,-1), 3),
            ("LEFTPADDING",   (0,0), (-1,-1), 4),
            ("RIGHTPADDING",  (0,0), (-1,-1), 4),
            ("VALIGN",        (0,0), (-1,-1), "TOP"),
            ("LINEBELOW",     (0,0), (-1,-1), 0.3, LIGHT_GREY),
            ("BOX",           (0,0), (-1,-1), 0.5, LIGHT_GREY),
        ])
        for i, bg in enumerate(row_colours):
            ts.add("BACKGROUND", (0, i), (-1, i), bg)
        checks_table.setStyle(ts)

        story.append(KeepTogether([header_table, checks_table]))
        story.append(Spacer(1, 5*mm))

    # ── Footer note ────────────────────────────────────────────────────────
    story.append(HRFlowable(width="100%", thickness=0.5, color=LIGHT_GREY, spaceBefore=2*mm, spaceAfter=2*mm))
    story.append(Paragraph(
        "All thresholds are configurable via healthcheck.yaml. "
        "Checks requiring extensions (amcheck, pg_visibility, pg_stat_statements, Spock) "
        "skip gracefully when not installed. "
        "G13-010 and G14-013 require local execution on the PostgreSQL host. "
        "G13-008 and G13-009 are Linux-only. "
        "github.com/ahsanhadi/pg_healthcheck",
        footer_style))

    doc.build(story)
    print(f"PDF written to: {OUTPUT}")

if __name__ == "__main__":
    build_pdf()
