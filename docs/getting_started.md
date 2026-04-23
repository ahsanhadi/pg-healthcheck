# Installation

pg_healthcheck is a single statically linked binary. The following
sections cover building from source and the prerequisites required
before running any checks.

## Prerequisites

The following software must be present before building or running
pg_healthcheck:

- Go 1.23 or later. Install with `brew install go` on macOS or follow
  the [Go installation guide](https://go.dev/doc/install).
- PostgreSQL 13 or later as the target database. Checks that require
  PostgreSQL 14, 15, 16, or 17 features skip gracefully on older
  versions.
- The `pg_monitor` built-in role on the target database. This role
  grants read access to all `pg_stat_*` views and catalog functions
  without requiring superuser. Some G11 security checks and G07
  `amcheck` verification return partial results without superuser.
- pgBackRest (optional). G02 backup checks skip gracefully with a
  clear message if pgBackRest is not installed.
- The `amcheck` extension (optional). G07 B-tree and heap integrity
  checks skip if the extension is absent. Install it with
  `CREATE EXTENSION amcheck;`.
- The pgEdge Spock extension (optional). G12 emits a single INFO
  finding and skips all checks if Spock is not installed; the tool
  is safe to run on standard PostgreSQL.

## Building from Source

Clone the repository and build the binary using the following
commands:

```bash
git clone https://github.com/ahsanhadi/pg_healthcheck.git
cd pg_healthcheck
go build -o pg_healthcheck ./cmd/pg_healthcheck
```

The resulting `pg_healthcheck` binary is self-contained and has no
runtime dependencies.

To verify the build, run the following command against a local
PostgreSQL instance:

```bash
./pg_healthcheck --host localhost --dbname postgres --user postgres
```

## Cross-Platform Builds

The tool builds for Linux (amd64, arm64), macOS (amd64, arm64), and
Windows (amd64). The CI pipeline verifies all platforms on every push.
To cross-compile manually, set the `GOOS` and `GOARCH` environment
variables before building:

```bash
GOOS=linux GOARCH=amd64 go build -o pg_healthcheck-linux-amd64 ./cmd/pg_healthcheck
```

## Running in CI

pg_healthcheck integrates naturally into CI pipelines using its exit
codes. A non-zero exit code signals health issues:

```bash
./pg_healthcheck --host $DB_HOST --user postgres && echo "healthy"
```

Exit code `1` indicates at least one WARN finding. Exit code `2`
indicates at least one CRITICAL finding. Exit code `0` means all
checks returned OK or INFO.

## Next Steps

- The [Configuration Reference](configuration.md) document explains
  every threshold in `healthcheck.yaml` and how to override defaults
  for your workload.
- The [CLI Usage and Modes](usage.md) document covers all command-line
  flags, operating modes, and practical examples.
