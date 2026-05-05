# Architecture

pg-healthcheck is structured as a pipeline of five discrete layers.
Each layer has a single responsibility, making it straightforward to
add new check groups or output formats without touching the others.

## Layer Overview

The following diagram shows how a single invocation flows through the
tool:

```
┌─────────────────────────────────────────────────────────────────┐
│  ./pg-healthcheck --host db1 --dbname mydb                       │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                ┌───────▼────────┐
                │   main.go      │  CLI layer - parses flags
                │   (CLI layer)  │  and orchestrates execution.
                └───────┬────────┘
                        │
           ┌────────────▼────────────┐
           │     config.go           │  Configuration layer - loads
           │  (Configuration layer)  │  healthcheck.yaml and applies
           └────────────┬────────────┘  CLI overrides on top.
                        │
      ┌─────────────────▼──────────────────┐
      │          connector/pg.go            │  Connection layer - opens
      │       (Database connection)         │  a PostgreSQL connection
      └─────────────────┬──────────────────┘  pool for each node.
                        │
      ┌─────────────────▼──────────────────┐
      │          Check Groups               │  Check layer - each group
      │   G01 through G15                  │  is an independent Go file
      │         checks/*.go                │  that queries system views
      └─────────────────┬──────────────────┘  and returns Findings.
                        │
      ┌─────────────────▼──────────────────┐
      │         report/reporter.go          │  Output layer - formats
      │         (Output layer)              │  Findings as terminal text
      └────────────────────────────────────┘  or JSON.
```

## CLI Layer

The CLI layer lives in `cmd/pg-healthcheck/main.go`. This file
contains `main()` and uses the Cobra library to define all command-line
flags. After parsing flags, `main()` calls `run()`, which orchestrates
the remaining layers in sequence. The CLI layer is the entry point and
sets the execution mode (`single` or `cluster`).

## Configuration Layer

The configuration layer lives in `internal/config/config.go`. The
`Config` struct holds every threshold the tool uses, such as WAL
directory size limits and replication lag thresholds. Safe production
defaults are hardcoded in the `Defaults()` function. The `Load()`
function reads `healthcheck.yaml` and merges values on top of the
defaults. CLI flags are applied last and override both. The final
`Config` value is passed to every check group.

## Connection Layer

The connection layer lives in `internal/connector/pg.go`. The layer
opens a `pgx` connection pool to each PostgreSQL node and hands it to
the check groups. In cluster mode, one pool is opened per node. Check
groups never open their own connections; they receive a pool and query
it using the provided context.

## Check Layer

The check layer lives in `internal/checks/`. Each check group is a
single Go file that implements the `Checker` interface:

```go
type Checker interface {
    GroupID() string   // e.g. "G05"
    Name()    string   // e.g. "Vacuum & Bloat"
    Run(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) ([]Finding, error)
}
```

Each call to `Run()` returns a slice of `Finding` values. A `Finding`
carries a severity level (`OK`, `INFO`, `WARN`, or `CRITICAL`), a
human-readable title and observation, an optional recommendation, and
an optional documentation URL. Check groups are independent and have
no shared state; adding a new group requires only implementing the
interface and registering the struct in `main.go`.

The `G12SpockCluster` group implements a separate `ClusterChecker`
interface that receives connections to all nodes simultaneously,
enabling cross-node comparisons.

## Output Layer

The output layer lives in `internal/report/reporter.go`. The layer
receives all `Finding` values from all check groups and formats them
either as coloured terminal text (the default) or as a JSON document.
The exit code is set based on the highest severity finding: `0` for
all OK or INFO, `1` for at least one WARN, and `2` for at least one
CRITICAL.

## Project Structure

The following table describes the layout of the repository:

| Path | Purpose |
|---|---|
| `cmd/pg-healthcheck/main.go` | CLI entry point - flags and orchestration |
| `internal/config/config.go` | Config struct, YAML loader, and defaults |
| `internal/connector/pg.go` | PostgreSQL connection pool helper |
| `internal/severity/severity.go` | OK, INFO, WARN, CRITICAL severity type |
| `internal/checks/checker.go` | Finding struct and Checker interface |
| `internal/checks/g01_connection.go` | Connection and availability checks |
| `internal/checks/g02_backrest.go` | pgBackRest backup checks |
| `internal/checks/g03_performance.go` | Performance and query stats checks |
| `internal/checks/g04_locks.go` | Long-running queries and lock checks |
| `internal/checks/g05_vacuum.go` | Vacuum and bloat checks |
| `internal/checks/g06_indexes.go` | Index health checks |
| `internal/checks/g07_toast.go` | TOAST and data integrity checks |
| `internal/checks/g08_visibility.go` | Visibility map checks |
| `internal/checks/g09_wal_slots.go` | WAL and replication slot checks |
| `internal/checks/g10_upgrade.go` | Upgrade readiness checks |
| `internal/checks/g11_security.go` | Security posture checks |
| `internal/checks/g12_spock.go` | pgEdge Spock cluster checks |
| `internal/checks/g13_os_resources.go` | OS and resource-level checks |
| `internal/checks/g14_wal_growth.go` | WAL growth and generation rate checks |
| `internal/checks/g15_replication_health.go` | Replication health checks |
| `internal/report/reporter.go` | Text and JSON output, exit codes |
| `healthcheck.yaml` | All tunable thresholds |
