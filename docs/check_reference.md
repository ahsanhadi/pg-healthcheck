# Check Groups Reference

pg-healthcheck organizes its 180+ checks into 15 groups, labeled G01
through G15. Each group focuses on a specific operational area and
runs independently. The following sections describe each group, its
purpose, and the individual checks it contains.

## Check Group Summary

The following table lists all groups with their check counts:

| Group | Name | Checks |
|---|---|---|
| G01 | Connection and Availability | 9 |
| G02 | pgBackRest Backup | 14 |
| G03 | Performance and Query Stats | 17 |
| G04 | Long-Running Queries and Lock Contention | 11 |
| G05 | Vacuum and Bloat | 11 |
| G06 | Indexes | 9 |
| G07 | TOAST and Data Integrity | 9 |
| G08 | Visibility Map | 5 |
| G09 | WAL and Replication Slots | 14 |
| G10 | Upgrade Readiness | 15 |
| G11 | Security Posture | 11 |
| G12 | pgEdge Spock Cluster | 20 |
| G13 | OS and Resource-Level Checks | 11 |
| G14 | WAL Growth and Generation Rate | 14 |
| G15 | Replication Health | 3 |

## G01 - Connection and Availability

G01 checks that the database is reachable, that connections are within
safe limits, and that the TLS certificate is not about to expire.

| Check ID | Title | Severity |
|---|---|---|
| G01-001 | TCP reachability | CRITICAL |
| G01-002 | Query round-trip time | WARN |
| G01-003 | Active connection saturation | WARN / CRITICAL |
| G01-004 | Idle-in-transaction session count | WARN |
| G01-005 | Max connections utilization | WARN / CRITICAL |
| G01-006 | TLS certificate expiry | WARN / CRITICAL |
| G01-007 | TLS certificate chain validity | WARN |
| G01-008 | Connection pooling advisory | INFO |
| G01-009 | pg_hba TRUST on non-loopback | CRITICAL |

## G02 - pgBackRest Backup

G02 checks pgBackRest backup recency, WAL archive health, and backup
retention. All checks skip gracefully if pgBackRest is not installed.

| Check ID | Title | Severity |
|---|---|---|
| G02-001 | Last full backup age | WARN / CRITICAL |
| G02-002 | Last differential backup age | WARN |
| G02-003 | WAL archive ready-file count | WARN / CRITICAL |
| G02-004 | Backup retention count | WARN |
| G02-005 | pgBackRest repository free space | WARN / CRITICAL |
| G02-006 | Archive push error log | WARN |
| G02-007 | backup-standby configuration | INFO |
| G02-008 | Backup compression | INFO |
| G02-009 | WAL archive mode | WARN |
| G02-010 | Stanza health | WARN |
| G02-011 | Restore test recency | INFO |
| G02-012 | Backup process running | INFO |
| G02-013 | Backup encryption | INFO |
| G02-014 | pgBackRest version | INFO |

## G03 - Performance and Query Stats

G03 inspects PostgreSQL configuration settings that affect query
performance, memory usage, and I/O behavior.

| Check ID | Title | Severity |
|---|---|---|
| G03-001 | shared_buffers size | INFO |
| G03-002 | I/O read latency from pg_stat_io | WARN |
| G03-003 | bgwriter buffer allocation rate | WARN |
| G03-004 | work_mem and max_connections product | WARN |
| G03-005 | maintenance_work_mem size | INFO |
| G03-006 | effective_cache_size setting | INFO |
| G03-007 | checkpoint_completion_target | WARN |
| G03-008 | Requested checkpoint ratio | WARN |
| G03-009 | wal_compression advisory | INFO |
| G03-010 | random_page_cost setting | INFO |
| G03-011 | effective_io_concurrency setting | INFO |
| G03-012 | JIT overhead from pg_stat_statements | INFO |
| G03-013 | wal_buffers size | INFO |
| G03-014 | default_statistics_target | INFO |
| G03-015 | Temp file spill from pg_stat_database | WARN |
| G03-016 | Database cache hit ratio | WARN / INFO |
| G03-017 | track_io_timing advisory | INFO |

## G04 - Long-Running Queries and Lock Contention

G04 detects active queries that are taking too long, sessions blocked
by locks, and configuration gaps that allow unbounded waits.

| Check ID | Title | Severity |
|---|---|---|
| G04-001 | Long-running active queries | WARN / CRITICAL |
| G04-002 | Idle-in-transaction session age | WARN |
| G04-003 | Lock blocker chains | WARN |
| G04-004 | Deadlock count from pg_stat_database | WARN |
| G04-005 | statement_timeout = 0 | WARN |
| G04-006 | idle_in_transaction_session_timeout = 0 | WARN |
| G04-007 | pg_stat_statements extension | WARN |
| G04-008 | Top queries by total execution time | INFO |
| G04-009 | log_min_duration_statement disabled | INFO |
| G04-010 | lock_timeout = 0 | WARN |
| G04-011 | Slow query count from pg_stat_statements | WARN / INFO |

## G05 - Vacuum and Bloat

G05 monitors transaction ID wraparound risk, dead tuple accumulation,
autovacuum staleness, and table bloat.

| Check ID | Title | Severity |
|---|---|---|
| G05-001 | Database transaction ID age | WARN / CRITICAL |
| G05-002 | Table transaction ID age | WARN / CRITICAL |
| G05-003 | Dead tuple ratio per table | WARN |
| G05-004 | Last autovacuum age per table | WARN |
| G05-005 | Autovacuum worker count | INFO |
| G05-006 | autovacuum_vacuum_scale_factor advisory | INFO |
| G05-007 | Table bloat heuristic | WARN |
| G05-008 | Autovacuum cost delay | INFO |
| G05-009 | Tables with no autovacuum ever | WARN |
| G05-010 | Multixact ID wraparound | WARN / CRITICAL |
| G05-011 | Frozen tuple ratio | INFO |

## G06 - Indexes

G06 detects indexes that are not being used, duplicate indexes that
waste storage and slow writes, and bloated indexes that harm query
performance.

| Check ID | Title | Severity |
|---|---|---|
| G06-001 | Unused indexes | INFO |
| G06-002 | Duplicate indexes | WARN |
| G06-003 | Invalid indexes | WARN |
| G06-004 | Bloated indexes | WARN |
| G06-005 | Missing primary keys | INFO |
| G06-006 | Indexes on low-cardinality columns | INFO |
| G06-007 | Partial index coverage | INFO |
| G06-008 | Foreign key columns without indexes | WARN |
| G06-009 | Index to table size ratio | INFO |

## G07 - TOAST and Data Integrity

G07 verifies that data checksums are enabled and runs structural
B-tree verification using the `amcheck` extension.

| Check ID | Title | Severity |
|---|---|---|
| G07-001 | Data checksums enabled | WARN |
| G07-002 | Checksum failure count | CRITICAL |
| G07-003 | TOAST table orphans | INFO |
| G07-004 | amcheck B-tree structural verification | CRITICAL |
| G07-005 | Oversized TOAST values | INFO |
| G07-006 | Tables without TOAST | INFO |
| G07-007 | TOAST compression method | INFO |
| G07-008 | Heap page corruption via amcheck | CRITICAL |
| G07-009 | Large TOAST ratio per table | INFO |

## G08 - Visibility Map

G08 checks the visibility map for anomalies that indicate stale vacuum
state, including tables with low all-visible ratios after recent
promotion.

| Check ID | Title | Severity |
|---|---|---|
| G08-001 | Heap blocks read ratio | WARN |
| G08-002 | All-visible page ratio per table | WARN |
| G08-003 | Tables with zero all-visible pages | INFO |
| G08-004 | Heap fetches from index-only scans | INFO |
| G08-005 | Dead tuple count vs update activity | WARN |

## G09 - WAL and Replication Slots

G09 checks replication slot health, streaming replication lag,
logical subscription worker status, and WAL retention settings.

| Check ID | Title | Severity |
|---|---|---|
| G09-001 | Inactive replication slot lag | WARN / CRITICAL |
| G09-002 | Retained WAL per slot | WARN / CRITICAL |
| G09-003 | Slot count vs max_replication_slots | WARN |
| G09-004 | Replication lag bytes | WARN / CRITICAL |
| G09-005 | Unnamed standbys | WARN |
| G09-006 | recovery_min_apply_delay advisory | INFO |
| G09-007 | wal_keep_size with physical standbys | WARN |
| G09-008 | Cross-reference to G02 archiving | INFO |
| G09-009 | Invalidated replication slots | CRITICAL |
| G09-010 | max_slot_wal_keep_size not configured | WARN |
| G09-011 | Inactive logical replication slots | WARN |
| G09-012 | Logical replication worker status | CRITICAL |
| G09-013 | Subscription relation sync state | WARN / CRITICAL |
| G09-014 | Streaming replication time-based lag | WARN / CRITICAL |

## G10 - Upgrade Readiness

G10 identifies extensions, data types, and configuration settings that
are incompatible with the next major PostgreSQL version.

| Check ID | Title | Severity |
|---|---|---|
| G10-001 | Legacy extensions installed | WARN |
| G10-002 | PostGIS version compatibility | WARN |
| G10-003 | Deprecated default_with_oids usage | WARN |
| G10-004 | sql_compat_function_member_of | INFO |
| G10-005 | pg_largeobject orphans | INFO |
| G10-006 | Deprecated GUC parameters | WARN |
| G10-007 | check_function_bodies = off | INFO |
| G10-008 | oid column usage in user tables | WARN |
| G10-009 | max_wal_senders advisory | INFO |
| G10-010 | Deprecated operator classes | INFO |
| G10-011 | Standard conforming strings | INFO |
| G10-012 | Encoding and collation | INFO |
| G10-013 | pg_upgrade compatibility blockers | WARN |
| G10-014 | Extensions not in contrib | INFO |
| G10-015 | pg_basebackup compatibility | INFO |

## G11 - Security Posture

G11 audits authentication configuration, privilege assignments, and
connection security settings.

| Check ID | Title | Severity |
|---|---|---|
| G11-001 | Superusers without password | CRITICAL |
| G11-002 | MD5 authentication entries | WARN |
| G11-003 | Public schema CREATE privilege | WARN |
| G11-004 | Privileged non-superuser roles | INFO |
| G11-005 | Connection logging | WARN |
| G11-006 | pgaudit extension | INFO |
| G11-007 | Stale login accounts | INFO |
| G11-008 | SSL certificate paths | INFO |
| G11-009 | Superuser login count | WARN / INFO |
| G11-010 | Password encryption method | WARN |
| G11-011 | Unencrypted client connections | WARN |

## G12 - pgEdge Spock Cluster

G12 checks Spock-specific cluster health across all nodes, including
subscription state, exception log size, conflict resolution counts,
and cross-node data parity. All checks skip gracefully if Spock is
not installed.

| Check ID | Title | Severity |
|---|---|---|
| G12-001 | Spock extension installed | INFO |
| G12-002 | Spock subscriptions enabled | WARN |
| G12-003 | Spock apply worker status | CRITICAL |
| G12-004 | Exception log row count | WARN / CRITICAL |
| G12-005 | Stale exception log entries | WARN |
| G12-006 | Conflict resolutions table size | WARN |
| G12-007 | Spock node membership | WARN |
| G12-008 | Cross-node row-count parity | WARN |
| G12-009 | Spock version consistency | WARN |
| G12-010 | Replication set membership | INFO |
| G12-011 | Forward origins configuration | WARN |
| G12-012 | Spock apply queue depth | WARN |
| G12-013 | Spock apply latency | WARN |
| G12-014 | Pending subscription sync | WARN |
| G12-015 | Conflict resolution mode | INFO |
| G12-016 | Spock DDL replication | INFO |
| G12-017 | Sequence synchronization | INFO |
| G12-018 | Spock worker crash recovery | WARN |
| G12-022 | Per-subscription conflict counts | WARN |
| G12-023 | Replication LSN lag between nodes | WARN |

## G13 - OS and Resource-Level Checks

G13 inspects operating system settings and PostgreSQL memory and I/O
behavior that affects stability and throughput.

| Check ID | Title | Severity |
|---|---|---|
| G13-001 | Checkpoint sync time | WARN |
| G13-002 | pg_stat_io buffer evictions | WARN |
| G13-003 | bgwriter maxwritten_clean | WARN |
| G13-004 | Huge pages configuration | WARN |
| G13-005 | Temp file spill | WARN |
| G13-006 | Query conflicts from pg_stat_database | WARN |
| G13-007 | max_connections advisory | WARN / INFO |
| G13-008 | Transparent Huge Pages (Linux) | WARN |
| G13-009 | CPU frequency governor (Linux) | WARN |
| G13-010 | Data directory disk space | WARN / CRITICAL |
| G13-011 | Postmaster uptime | WARN / INFO |

G13-010 and G13-011 require pg-healthcheck to run on the PostgreSQL
host. Remote executions receive a graceful INFO skip.

## G14 - WAL Growth and Generation Rate

G14 monitors WAL directory size, generation rate against a rolling
baseline, full-page write ratio, and archive status.

| Check ID | Title | Severity |
|---|---|---|
| G14-001 | pg_wal directory size | WARN / CRITICAL |
| G14-002 | Live WAL generation rate | WARN / CRITICAL |
| G14-003 | WAL rate vs rolling baseline | WARN |
| G14-004 | pg_stat_wal summary | INFO |
| G14-005 | Full-page write ratio | WARN |
| G14-006 | Top WAL-generating tables | INFO |
| G14-007 | wal_compression advisory | INFO |
| G14-008 | wal_level=logical with no consumers | WARN |
| G14-009 | WAL segment file count | WARN |
| G14-010 | WAL archiver status and recency | WARN / CRITICAL |
| G14-011 | UNLOGGED tables advisory | INFO |
| G14-012 | Forced checkpoint rate | WARN |
| G14-013 | pg_wal filesystem percentage | WARN / CRITICAL |
| G14-014 | Long transactions blocking WAL recycling | WARN |

G14-010 uses `last_failed_time` recency rather than a cumulative
failure count, so old resolved archive failures do not produce
perpetual alerts. A failure within the last hour triggers CRITICAL;
a failure within the last 24 hours triggers WARN.

## G15 - Replication Health

G15 checks streaming replication connectivity from both the standby
and primary perspectives, and provides a per-type conflict breakdown
for standby nodes.

| Check ID | Title | Severity |
|---|---|---|
| G15-001 | WAL receiver status | CRITICAL / WARN |
| G15-002 | Inactive physical replication slots | CRITICAL |
| G15-003 | Standby query conflicts by type | WARN |

G15-001 runs on standby nodes only. It reports CRITICAL if the WAL
receiver process is absent and WARN if no message has been received
from the primary in more than 5 minutes.

G15-002 runs on primary nodes only. Physical slots with `active =
false` represent disconnected standbys. Unlike inactive logical slots
(G09-001), physical slot lag is not caught by `confirmed_flush_lsn`
checks, making this a distinct gap that G15-002 fills.

G15-003 runs on standby nodes only. The check queries
`pg_stat_database_conflicts` and breaks down conflict counts by type:
`confl_snapshot`, `confl_lock`, `confl_bufferpin`, `confl_deadlock`,
and `confl_active_logicalslot` (PostgreSQL 16+). Each type points to
a different root cause:

- `confl_snapshot` spikes indicate that the primary vacuum is
  reclaiming rows still visible to standby queries; enabling
  `hot_standby_feedback=on` reduces these conflicts.
- `confl_bufferpin` spikes indicate that long-running standby queries
  are holding buffer pins.
- `confl_active_logicalslot` spikes indicate that a logical decoding
  slot is blocking standby cleanup on PostgreSQL 16 or later.
