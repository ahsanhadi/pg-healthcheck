# Configuration Reference

pg_healthcheck loads thresholds from a YAML configuration file. All
keys are optional - absent keys keep their built-in defaults. Pass
the file path with `--config /path/to/healthcheck.yaml`. CLI flags
override YAML values, which in turn override built-in defaults.

## File Format

The configuration file uses YAML with the following conventions:

- String values for paths do not require quotes.
- Lists use YAML sequence syntax (one item per line with a `-` prefix).
- Numbers are plain integers or decimals without quotes.
- Comments start with `#`.

If the file contains a syntax error, pg_healthcheck prints a warning
and falls back to built-in defaults:

```
config warning: parsing config prod.yaml: yaml: line 12: ...
```

## Connection (G01)

The following settings control connection availability thresholds:

```yaml
connection_timeout_ms:    5000  # milliseconds to wait for a TCP connection
pg_isready_warn_ms:        500  # WARN if SELECT 1 round-trip exceeds this
warn_connections_pct:       75  # WARN when connections exceed 75% of max
critical_connections_pct:   90  # CRITICAL when connections exceed 90% of max
idle_in_tx_warn_seconds:    30  # WARN on idle-in-transaction sessions
```

## TLS Certificates (G01)

The following settings control TLS certificate expiry thresholds:

```yaml
ssl_cert_warn_days:      30  # WARN when TLS cert expires within 30 days
ssl_cert_critical_days:   7  # CRITICAL when cert expires within 7 days
```

## pgBackRest Backup (G02)

The following settings configure pgBackRest integration:

```yaml
backrest_config:   /etc/pgbackrest/pgbackrest.conf
backrest_stanza:   main       # run `pgbackrest info` to find your stanza name
backup_max_age_hours:     26  # WARN if no successful backup in 26 hours
min_retention_full:        2  # WARN if fewer than 2 full backups exist
wal_ready_warn_count:    100  # WARN if >100 WAL files await archiving
wal_ready_critical_count: 500 # CRITICAL if >500 WAL files await archiving
```

The `backrest_stanza` value is the most commonly changed setting.
Run `pgbackrest info` to find the stanza name for your cluster.

## Queries and Locks (G03, G04)

The following settings control query duration and slow query
detection thresholds:

```yaml
long_query_warn_seconds:     60    # WARN on queries running longer than 1 min
long_query_critical_seconds: 300   # CRITICAL on queries longer than 5 min
slow_query_mean_warn_ms:     5000  # WARN if >10 query patterns have mean
                                   # execution time above this threshold (ms)
```

The `slow_query_mean_warn_ms` threshold requires the
`pg_stat_statements` extension. A minimum of 5 executions is required
before a query pattern is evaluated.

## Vacuum and TXID Wraparound (G05)

The following settings control transaction ID wraparound thresholds:

```yaml
txid_wrap_warn_million:     500  # WARN when fewer than 500M XIDs remain
txid_wrap_critical_million: 200  # CRITICAL when fewer than 200M remain
```

Tighten these values on high-write databases. On a read-heavy replica
with frequent false positives, the values can be raised.

## WAL and Replication Slots (G09)

The following settings control replication lag and slot retention
thresholds:

```yaml
replication_lag_warn_bytes:     52428800   # WARN at 50 MB of lag
replication_lag_critical_bytes: 524288000  # CRITICAL at 500 MB of lag
wal_slot_retain_warn_gb:      5            # WARN when a slot retains >5 GB
wal_slot_retain_critical_gb:  20           # CRITICAL when retaining >20 GB
```

## pgEdge Spock Cluster (G12)

The following settings configure Spock-specific checks and cross-node
row-count sampling:

```yaml
spock_exception_log_warn_rows:   10000  # WARN at >10k exception log rows
spock_exception_log_crit_rows:  100000  # CRITICAL at 100k rows
spock_resolutions_warn_rows:     50000  # WARN at >50k resolutions rows
spock_old_exception_days:            7  # WARN on unresolved exceptions >7 days

cross_node_count_threshold_pct: 1.0    # WARN if row counts differ by >1%
cross_node_tables:                     # tables to sample for row-count parity
  - public.orders
  - public.accounts
```

## amcheck - B-tree Structural Verification (G07)

The following setting lists tables to run structural B-tree checks on:

```yaml
amcheck_table_list:
  - public.orders
  - public.accounts
```

Leave this as `[]` to skip amcheck entirely. The `amcheck` extension
must be installed: `CREATE EXTENSION amcheck;`.

## WAL Growth and Generation Rate (G14)

The following settings control WAL size, generation rate, and
archiving thresholds:

```yaml
wal_rate_warn_mb_s:            50   # WARN if WAL generates faster than 50 MB/s
wal_rate_critical_mb_s:       200   # CRITICAL at 200 MB/s
wal_dir_warn_gb:               20   # WARN if pg_wal directory exceeds 20 GB
wal_dir_critical_gb:           50   # CRITICAL if it exceeds 50 GB
wal_rate_baseline_multiplier:  3.0  # WARN if rate is >3x the rolling average
wal_rate_baseline_samples:      12  # samples to keep for the rolling average
wal_fpi_ratio_warn:            0.40 # WARN if FPI records exceed 40% of WAL
wal_filesystem_warn_pct:        60  # WARN if pg_wal filesystem is >60% full
wal_filesystem_critical_pct:    80  # CRITICAL at >80% - exhaustion crashes PG
wal_rate_state_file: /var/lib/pg_healthcheck/wal_rate.json
```

Set `wal_rate_state_file` to a persistent path outside `/tmp/`. Files
in `/tmp/` are cleared on reboot, resetting the rolling baseline and
producing false spike alerts on startup.

## Per-Check Timeout

The following setting limits how long each individual check runs:

```yaml
check_timeout_seconds: 10  # cancel each check after this many seconds
```

Increase this value to `30` when connecting over a slow network or
when the database is under heavy load and catalog queries are slow.

## Minimal Production Example

The following file shows only the keys that differ from defaults for
a typical production server:

```yaml
# /etc/pg_healthcheck/prod.yaml

backup_max_age_hours:        13
backrest_stanza:             prod-db

txid_wrap_warn_million:      300
txid_wrap_critical_million:  100

wal_dir_warn_gb:             40
wal_dir_critical_gb:         70
wal_filesystem_warn_pct:     50
wal_filesystem_critical_pct: 70

wal_rate_state_file: /var/lib/pg_healthcheck/wal_rate.json

check_timeout_seconds: 30
```
