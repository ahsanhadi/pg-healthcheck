# pg_healthcheck

> Enterprise-grade PostgreSQL health diagnostics for single instances and pgEdge multi-node Spock clusters.

![CI](https://github.com/ahsanhadi/pg_healthcheck/actions/workflows/ci.yml/badge.svg)
![Go](https://img.shields.io/badge/Go-1.23+-00ADD8?logo=go&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13+-336791?logo=postgresql&logoColor=white)
![License](https://img.shields.io/badge/license-PostgreSQL-blue)
![Releases](https://img.shields.io/github/v/release/ahsanhadi/pg_healthcheck)

Runs **115+ checks across 14 groups** and queries live PostgreSQL system catalog views — no estimates, no simulated data. Output is coloured terminal text or structured JSON for GUI/API consumption.

---

## How the App Works — Architecture

If you are new to Go, this section explains how all the pieces fit together before you start reading any code.

```
┌─────────────────────────────────────────────────────────────────┐
│  You run:  ./pg_healthcheck --host db1 --dbname mydb             │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                    ┌───────▼────────┐
                    │   main.go      │  ← Entry point. Parses flags
                    │   (CLI layer)  │    and kicks everything off.
                    └───────┬────────┘
                            │
               ┌────────────▼────────────┐
               │     config.go           │  ← Loads healthcheck.yaml +
               │  (Configuration layer)  │    applies CLI flags on top.
               └────────────┬────────────┘
                            │
          ┌─────────────────▼──────────────────┐
          │          connector/pg.go            │  ← Opens a PostgreSQL
          │       (Database connection)         │    connection pool.
          └─────────────────┬──────────────────┘
                            │
          ┌─────────────────▼──────────────────┐
          │          Check Groups               │  ← The core work happens
          │   G01 → G14  (14 groups, 100+ checks)   here. Each group is an
          │         checks/*.go                 │    independent Go file.
          └─────────────────┬──────────────────┘
                            │  (list of Findings)
          ┌─────────────────▼──────────────────┐
          │         report/reporter.go          │  ← Formats and prints the
          │         (Output layer)              │    results as text or JSON.
          └────────────────────────────────────┘
```

### The five layers explained simply

**1. CLI layer (`cmd/pg_healthcheck/main.go`)**
This is where `main()` lives. It uses a library called Cobra to define all the `--flags` you pass on the command line. Once the flags are parsed, it calls `run()` which orchestrates everything else. Think of this as the "front door" of the application.

**2. Configuration layer (`internal/config/config.go`)**
There is one central `Config` struct that holds every threshold the tool uses — things like "warn when WAL dir > 20 GB" or "critical when TLS cert expires within 7 days". Defaults are hardcoded. You can override any value with `healthcheck.yaml`. CLI flags override the YAML. The final Config is handed to every check.

**3. Database connection (`internal/connector/pg.go`)**
Opens a PostgreSQL connection pool using the `pgx` library. In cluster mode, it opens one pool per node. The pool is passed to every check — checks never open their own connections.

**4. Check groups (`internal/checks/`)**
This is where all the actual work happens. Each group (G01, G02, … G14) is one Go file. They all follow the same simple pattern:

```
┌──────────────────────────────────────────────────────────┐
│  Every check group implements the Checker interface:      │
│                                                           │
│  type Checker interface {                                 │
│      GroupID() string          ← e.g. "G05"              │
│      Name()    string          ← e.g. "Vacuum & Bloat"   │
│      Run(ctx, db, cfg)         ← runs the SQL queries     │
│          ([]Finding, error)       and returns results     │
│  }                                                        │
└──────────────────────────────────────────────────────────┘
```

Each check returns one or more **Findings**. A Finding is a simple struct:

```
Finding {
    CheckID     "G05-001"                   ← unique check ID
    Group       "Vacuum & Bloat"            ← which group
    Severity    OK / INFO / WARN / CRITICAL ← how bad is it?
    Title       "TXID wraparound"           ← what was checked
    Observed    "450M transactions left"    ← what was found
    Recommended "Run VACUUM FREEZE"         ← what to do
    Detail      "..."                       ← extra context
    DocURL      "https://postgresql.org/…"  ← link to docs
    NodeName    "node1:5432"                ← (cluster mode only)
}
```

**5. Output layer (`internal/report/reporter.go`)**
Takes the full list of Findings and either:
- Prints coloured terminal text grouped by check group, sorted by severity
- Writes a single JSON object to stdout (for GUI / API consumption)

Also responsible for composite alerts — if two related groups are simultaneously CRITICAL (e.g. G02 archiving + G14 WAL growth), it prints a combined banner explaining the combined danger.

---

### How a single check actually works

Here is a simplified real example from G13 (`g13_os_resources.go`):

```go
// Check if the background writer had to stop mid-scan
func g13MaxwrittenClean(ctx context.Context, db *pgxpool.Pool) []Finding {

    // 1. Run a SQL query against a PostgreSQL catalog view
    var count int64
    db.QueryRow(ctx, "SELECT maxwritten_clean FROM pg_stat_bgwriter").Scan(&count)

    // 2. Compare the result against a threshold
    if count > 0 {
        // 3a. Return a WARN Finding with a recommendation
        return []Finding{NewWarn("G13-003", g13, "maxwritten_clean",
            fmt.Sprintf("maxwritten_clean = %d", count),
            "Increase bgwriter_lru_maxpages or shared_buffers.",
            "bgwriter is stopping cleaning passes mid-scan.",
            "https://postgresql.org/docs/...")}
    }

    // 3b. Return an OK Finding
    return []Finding{NewOK("G13-003", g13, "maxwritten_clean",
        fmt.Sprintf("maxwritten_clean = %d", count),
        "https://postgresql.org/docs/...")}
}
```

Every single check in the codebase follows this exact pattern. If you want to add a new check, you just add a function like this to the right group file and call it from that group's `Run()` method.

---

### Severity levels

| Level | Meaning |
|---|---|
| ✓ **OK** | Everything is fine |
| ⓘ **INFO** | Advisory — good to know, no action urgently needed |
| ⚠ **WARN** | Should be fixed before the next incident window |
| ✗ **CRITICAL** | Requires immediate attention |

Exit codes: `0` = all OK · `1` = any WARN · `2` = any CRITICAL

---

### WAL health — the three-group picture

Three groups together cover the full WAL lifecycle:

```
  WAL is written here          WAL sits on disk here       WAL leaves the server
  ┌──────────────────┐         ┌────────────────────┐      ┌───────────────────┐
  │ G14 WAL Growth   │ ──────► │  pg_wal directory  │ ───► │  G02 pgBackRest   │
  │ (generation rate)│         │  G09 slot retention│      │  (archive pipeline│
  └──────────────────┘         └────────────────────┘      └───────────────────┘
```

- **G02** — monitors whether WAL is leaving the server successfully (archiving)
- **G09** — monitors whether WAL is being held back by inactive replication slots
- **G14** — monitors WAL as a raw resource: how fast it is produced and how much disk it occupies

If two of these are simultaneously CRITICAL, the reporter prints a **composite alert** explaining the combined danger.

---

## Quick Start

### Download a release (no Go required)

Pre-built binaries for Linux, macOS, and Windows are available on the [Releases page](https://github.com/ahsanhadi/pg_healthcheck/releases).

```bash
# macOS (Apple Silicon)
curl -L https://github.com/ahsanhadi/pg_healthcheck/releases/latest/download/pg_healthcheck_macOS_arm64.tar.gz | tar xz

# macOS (Intel)
curl -L https://github.com/ahsanhadi/pg_healthcheck/releases/latest/download/pg_healthcheck_macOS_amd64.tar.gz | tar xz

# Linux (amd64)
curl -L https://github.com/ahsanhadi/pg_healthcheck/releases/latest/download/pg_healthcheck_linux_amd64.tar.gz | tar xz
```

Each archive includes the binary, `LICENSE`, `README.md`, and a ready-to-edit `healthcheck.yaml`.

### Build from source

```bash
git clone https://github.com/ahsanhadi/pg_healthcheck.git
cd pg_healthcheck
go build -o pg_healthcheck ./cmd/pg_healthcheck/
```

Requires Go 1.23+. Install with `brew install go` on macOS or `apt install golang-go` on Ubuntu.

### Run against a local database

```bash
./pg_healthcheck --host localhost --port 5432 --dbname mydb --user postgres
```

### Password — use an environment variable

```bash
PGPASSWORD=secret ./pg_healthcheck --host db1 --dbname prod --user postgres
```

### Run only specific groups

```bash
./pg_healthcheck --groups G01,G05,G09,G14
```

### Show all checks including OK ones

```bash
./pg_healthcheck --verbose
```

### JSON output (for GUI or scripting)

```bash
./pg_healthcheck --output json | jq '.summary'
./pg_healthcheck --output json > report.json
```

### Cluster mode (pgEdge / Spock)

```bash
./pg_healthcheck \
  --mode cluster \
  --nodes node1:5432,node2:5432,node3:5432 \
  --dbname mydb --user postgres
```

> **Note:** If two entries in `--nodes` resolve to the same database (e.g. during testing
> with a single node), duplicate findings are automatically suppressed — each check
> appears exactly once per unique node.

### Upgrade readiness check

```bash
./pg_healthcheck --groups G10 --target-version 17
```

---

## All Flags

| Flag | Default | Description |
|---|---|---|
| `--host` | `localhost` | PostgreSQL host |
| `--port` | `5432` | PostgreSQL port |
| `--dbname` | `postgres` | Database name |
| `--user` | `postgres` | Role name (or `PGUSER` env) |
| `--password` | `` | Password (prefer `PGPASSWORD` env) |
| `--mode` | `single` | `single` or `cluster` |
| `--nodes` | | Comma-separated `host:port` list (cluster mode) |
| `--config` | | Path to YAML config file |
| `--output` | `text` | `text` or `json` |
| `--groups` | all | Comma-separated group IDs, e.g. `G01,G05,G14` |
| `--target-version` | `0` | Target PG major version for G10 upgrade checks |
| `--backrest-config` | | Path to `pgbackrest.conf` |
| `--no-color` | false | Disable terminal colour |
| `--verbose` | false | Show OK findings (hidden by default) |

---

## Configuration File (`healthcheck.yaml`)

### How configuration loading works

Thresholds are applied in this order — later steps always win:

```
1. Built-in defaults (safe baselines hardcoded in config.go)
        ↓
2. healthcheck.yaml  (your environment-specific overrides)
        ↓
3. CLI flags         (one-off overrides for a single run)
```

You never have to edit the file. But tuning it is how you make the tool fit your environment rather than the defaults.

### Where to put the file

The tool looks for `healthcheck.yaml` in the **current working directory** automatically. To use a different path pass `--config`:

```bash
./pg_healthcheck --config /etc/pg_healthcheck/prod.yaml
```

A common pattern is one file per environment:

```
/etc/pg_healthcheck/
    prod.yaml
    staging.yaml
    dev.yaml
```

### YAML editing rules

- Use **2 spaces** for indentation — no tabs
- You only need to include the keys you want to change. Omitted keys keep their built-in default
- Lists can be written inline `["a", "b"]` or as block items:
  ```yaml
  cross_node_tables:
    - public.orders
    - public.users
  ```
- Numbers are plain integers or decimals — no quotes needed
- Comments start with `#`

If the file has a syntax error the tool will print a warning and fall back to built-in defaults:
```
config warning: parsing config prod.yaml: yaml: line 12: ...
```

### Test your file before deploying

```bash
./pg_healthcheck --config /etc/pg_healthcheck/prod.yaml --groups G01 --verbose
```

---

### All configuration keys explained

#### Connection (G01)

```yaml
connection_timeout_ms:    5000   # milliseconds to wait for a TCP connection
pg_isready_warn_ms:        500   # WARN if SELECT 1 round-trip takes longer than this
warn_connections_pct:       75   # WARN when active connections exceed 75% of max_connections
critical_connections_pct:   90   # CRITICAL when active connections exceed 90% of max_connections
idle_in_tx_warn_seconds:    30   # WARN on sessions sitting idle-in-transaction longer than this
```

> **Tip:** On a busy OLTP server with a connection pooler (PgBouncer), you can safely raise
> `warn_connections_pct` to 85 since the pooler manages bursts.

#### TLS certificates (G01)

```yaml
ssl_cert_warn_days:      30   # WARN when the server TLS cert expires within 30 days
ssl_cert_critical_days:   7   # CRITICAL when the cert expires within 7 days
```

#### pgBackRest backup (G02)

```yaml
backrest_config:   /etc/pgbackrest/pgbackrest.conf   # path to your pgbackrest.conf
backrest_stanza:   main                              # stanza name — run `pgbackrest info` to find yours
backup_max_age_hours:     26   # WARN if no successful backup in the last 26 hours
min_retention_full:        2   # WARN if fewer than 2 full backups exist
wal_ready_warn_count:    100   # WARN if >100 WAL files are waiting to be archived
wal_ready_critical_count: 500  # CRITICAL if >500 WAL files waiting
```

> **Tip:** `backrest_stanza` is the most common thing to change. Check your pgbackrest.conf
> or run `pgbackrest info` — the stanza name appears at the top of the output.

#### Queries & locks (G03, G04)

```yaml
long_query_warn_seconds:     60   # WARN on queries running longer than 1 minute
long_query_critical_seconds: 300  # CRITICAL on queries running longer than 5 minutes
```

#### Vacuum & TXID wraparound (G05)

```yaml
txid_wrap_warn_million:     500   # WARN when fewer than 500M transaction IDs remain
txid_wrap_critical_million: 200   # CRITICAL when fewer than 200M remain
```

> **Tip:** Tighten these on high-write databases (lower the numbers). If you see frequent
> false positives on a read-heavy replica, you can safely raise them.

#### WAL & replication slots (G09)

```yaml
replication_lag_warn_bytes:     52428800    # WARN at  50 MB of replication lag
replication_lag_critical_bytes: 524288000   # CRITICAL at 500 MB
wal_slot_retain_warn_gb:      5    # WARN when a slot is retaining > 5 GB of WAL
wal_slot_retain_critical_gb:  20   # CRITICAL when retaining > 20 GB
```

> **New in G09:** Checks G09-009 through G09-013 cover logical replication slot health —
> invalidated slots, missing workers, and subscription relation sync state. These fire
> automatically; no additional YAML configuration is required.

#### pgEdge / Spock cluster (G12)

```yaml
spock_exception_log_warn_rows:   10000   # WARN if spock exception log has > 10k rows
spock_exception_log_crit_rows:  100000   # CRITICAL at 100k rows
spock_resolutions_warn_rows:     50000   # WARN if resolutions table has > 50k rows
spock_old_exception_days:            7   # WARN on unresolved exceptions older than 7 days

cross_node_count_threshold_pct: 1.0   # WARN if row counts differ by more than 1% between nodes
cross_node_tables:                    # tables to sample for row-count parity (leave empty to skip)
  - public.orders
  - public.accounts
```

#### amcheck — B-tree structural verification (G07)

```yaml
amcheck_table_list:          # tables to run structural B-tree checks on
  - public.orders
  - public.accounts
```

> Leave as `[]` to skip amcheck entirely. Add your most critical indexed tables here.
> Requires the `amcheck` extension: `CREATE EXTENSION amcheck;`

#### WAL growth & generation rate (G14)

```yaml
wal_rate_warn_mb_s:            50    # WARN if WAL is generating faster than 50 MB/s
wal_rate_critical_mb_s:       200    # CRITICAL at 200 MB/s

wal_dir_warn_gb:               20    # WARN if the pg_wal directory exceeds 20 GB
wal_dir_critical_gb:           50    # CRITICAL if it exceeds 50 GB

wal_rate_baseline_multiplier:  3.0   # WARN if current rate is >3× the rolling average
wal_rate_baseline_samples:      12   # how many past samples to keep for the rolling average
                                     # (12 samples × run frequency = your baseline window)

wal_fpi_ratio_warn:            0.40  # WARN if full-page writes exceed 40% of all WAL records

wal_filesystem_warn_pct:        60   # WARN if the pg_wal filesystem is >60% full
wal_filesystem_critical_pct:    80   # CRITICAL at >80% — pg_wal exhaustion crashes PostgreSQL

wal_rate_state_file: /var/lib/pg_healthcheck/wal_rate.json   # where to store the rolling baseline
```

> **Important:** Change `wal_rate_state_file` from `/tmp/` to a persistent path like
> `/var/lib/pg_healthcheck/`. Files in `/tmp/` are cleared on reboot and the rolling
> baseline resets, giving false spike alerts on startup.
>
> Set `wal_dir_warn_gb` to roughly 40% of your actual pg_wal partition size, and
> `wal_dir_critical_gb` to 70%.

#### Per-check timeout

```yaml
check_timeout_seconds: 10   # each individual check is cancelled after this many seconds
```

> Increase to `30` if the tool is connecting over a slow network or the database is under
> heavy load and catalog queries are slow.

---

### Minimal example for a production server

You do not need to include every key — only what differs from the defaults:

```yaml
# /etc/pg_healthcheck/prod.yaml

# Our backups run every 12 hours
backup_max_age_hours:        13
backrest_stanza:             prod-db

# Tighter wraparound thresholds for our high-write workload
txid_wrap_warn_million:      300
txid_wrap_critical_million:  100

# pg_wal lives on a 100 GB dedicated volume
wal_dir_warn_gb:             40
wal_dir_critical_gb:         70
wal_filesystem_warn_pct:     50
wal_filesystem_critical_pct: 70

# Persistent baseline file
wal_rate_state_file: /var/lib/pg_healthcheck/wal_rate.json

# Slow network — give queries more time
check_timeout_seconds: 30
```

---

## Check Groups

| Group | Name | Checks |
|---|---|---|
| G01 | Connection & Availability | 9 |
| G02 | pgBackRest Backup | 14 |
| G03 | Performance & Query Stats | 15 |
| G04 | Locks & Blocking | 10 |
| G05 | Vacuum & Bloat | 11 |
| G06 | Indexes | 9 |
| G07 | TOAST & Data Integrity | 9 |
| G08 | Visibility Map | 5 |
| G09 | WAL & Replication Slots | 13 |
| G10 | Upgrade Readiness | 15 |
| G11 | Security | 8 |
| G12 | pgEdge / Spock Cluster | 20 |
| G13 | OS & Resource-Level | 7 |
| G14 | WAL Growth & Generation Rate | 14 |

### G09 — WAL & Replication Slots (recent additions)

| Check | What it detects |
|---|---|
| G09-009 | Invalidated logical replication slots (PG 16+ marks slots invalid when WAL is gone) |
| G09-010 | `max_slot_wal_keep_size` not set — slots can retain unlimited WAL and fill the disk |
| G09-011 | Inactive logical replication slots older than 1 hour |
| G09-012 | Logical replication worker status — workers not running for active subscriptions |
| G09-013 | Subscription relation sync state — tables stuck in error or non-ready state |

### G12 — pgEdge / Spock Cluster (recent additions)

| Check | What it detects |
|---|---|
| G12-022 | Per-subscription conflict and DCA counters from `spock.channel_summary_stats` |
| G12-023 | Replication LSN lag in MB between each node pair from `spock.progress` |

> All G12 Spock catalog queries have been verified against live pgEdge Spock schema.
> Checks that reference tables or columns not present on the installed Spock version
> skip gracefully with an INFO message rather than erroring.

### G14 checks at a glance

| Check | What it detects |
|---|---|
| G14-001 | pg_wal directory size vs configured GB thresholds |
| G14-002 | Live WAL generation rate in MB/s |
| G14-003 | Current rate vs rolling baseline (detects sudden spikes) |
| G14-004 | WAL statistics summary from pg_stat_wal (PG 14+) |
| G14-005 | Full-page write ratio — warns when FPI > 40% of all records |
| G14-006 | Top 5 WAL-generating tables by modification count |
| G14-007 | wal_compression advisory |
| G14-008 | wal_level=logical with no logical consumers (wastes WAL) |
| G14-009 | WAL segment file count (high count = recycling blocked) |
| G14-010 | WAL archiver status and time since last successful archive |
| G14-011 | UNLOGGED tables (converting them causes a WAL spike) |
| G14-012 | Forced checkpoint rate (checkpoints_req > 20% = max_wal_size too small) |
| G14-013 | pg_wal filesystem percentage (CRITICAL at 80% — no graceful degradation) |
| G14-014 | Long transactions blocking WAL segment recycling |

---

## JSON Output Schema

```json
{
  "timestamp": "2025-01-15T10:30:00Z",
  "hostname": "db1:5432",
  "pg_version": "16.2",
  "mode": "single",
  "summary": {
    "ok": 88,
    "info": 5,
    "warn": 2,
    "critical": 0,
    "total": 95
  },
  "checks": [
    {
      "check_id": "G14-002",
      "group": "WAL Growth & Generation Rate",
      "severity": "WARN",
      "title": "WAL generation rate",
      "observed": "WAL rate: 67.3 MB/s  (over 2.1s sample)",
      "recommended": "Identify top WAL-generating tables; look for bulk writes or FPI storms.",
      "detail": "",
      "doc_url": "https://www.postgresql.org/docs/current/wal-configuration.html",
      "node_name": ""
    }
  ]
}
```

---

## Exit Codes

```
0  — all checks passed (or only INFO)
1  — at least one WARN finding
2  — at least one CRITICAL finding
```

Use in scripts and CI:

```bash
./pg_healthcheck --host prod-db && echo "healthy" || echo "issues found (exit $?)"
```

---

## Project Structure

```
pg_healthcheck/
│
├── cmd/pg_healthcheck/
│   └── main.go                  CLI entry point — flags, orchestration
│
├── internal/
│   ├── config/
│   │   └── config.go            Config struct, YAML loader, defaults
│   │
│   ├── connector/
│   │   └── pg.go                PostgreSQL connection pool helper
│   │
│   ├── severity/
│   │   └── severity.go          OK / INFO / WARN / CRITICAL type
│   │
│   ├── checks/
│   │   ├── checker.go           Finding struct + Checker interface
│   │   ├── g01_connection.go    Connection & availability (9 checks)
│   │   ├── g02_backrest.go      pgBackRest backup (14 checks)
│   │   ├── g03_performance.go   Performance & query stats (15 checks)
│   │   ├── g04_locks.go         Locks & blocking (10 checks)
│   │   ├── g05_vacuum.go        Vacuum & bloat (11 checks)
│   │   ├── g06_indexes.go       Indexes (9 checks)
│   │   ├── g07_toast.go         TOAST & data integrity (9 checks)
│   │   ├── g08_visibility.go    Visibility map (5 checks)
│   │   ├── g09_wal_slots.go     WAL & replication slots (13 checks)
│   │   ├── g10_upgrade.go       Upgrade readiness (15 checks)
│   │   ├── g11_security.go      Security (8 checks)
│   │   ├── g12_spock.go         pgEdge / Spock cluster (20 checks)
│   │   ├── g13_os_resources.go  OS & resource-level (7 checks)
│   │   └── g14_wal_growth.go    WAL growth & generation rate (14 checks)
│   │
│   └── report/
│       └── reporter.go          Text + JSON output, composite alerts, exit code
│
├── healthcheck.yaml             All tunable thresholds (copy and customise)
├── .goreleaser.yaml             GoReleaser — multi-platform release builds
├── .golangci.yml                golangci-lint configuration
├── .github/
│   └── workflows/
│       ├── ci.yml               CI — lint, vet, build, test on every push/PR
│       └── release.yml          Release — builds & publishes binaries on v* tags
├── go.mod
└── README.md
```

---

## Requirements

- **Go 1.23+** — install with `brew install go`
- **PostgreSQL 13+** — checks that need PG 14/15/16 skip gracefully on older versions
- **pg_monitor role** — recommended minimum privilege for the healthcheck user
- **pgbackrest** binary in `PATH` — G02 checks skip gracefully if absent
- **amcheck extension** — G07 B-tree integrity check skips if not installed
- **pgEdge Spock extension** — G12 checks skip gracefully if Spock is not installed
- **Same host as PostgreSQL** — required only for G14-013 filesystem check (uses `syscall.Statfs`)

---

## CI & Releases

Every push and pull request to `main` runs the full CI pipeline:

- **gofmt** — formatting check
- **go vet** — static analysis
- **golangci-lint** — errcheck, staticcheck, unused, ineffassign
- **Cross-compile** — verified to build on linux/amd64, linux/arm64, darwin/amd64, darwin/arm64, windows/amd64
- **go test -race** — race detector enabled

### Cutting a release

Tag the commit and push — GoReleaser does the rest:

```bash
git tag v0.2.0
git push origin v0.2.0
```

This automatically builds binaries for all platforms, packages each one with `LICENSE`, `README.md`, and `healthcheck.yaml`, and publishes a GitHub Release with a generated changelog.

---

## License

pg_healthcheck is released under the [PostgreSQL License](LICENSE) — the same permissive license used by PostgreSQL itself.

Copyright (c) 2025, Ahsan Hadi
