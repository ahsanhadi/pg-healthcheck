# CLI Usage and Modes

pg-healthcheck supports two operating modes: single-node mode for
diagnosing one PostgreSQL instance, and cluster mode for diagnosing
a pgEdge multi-node Spock cluster. The following sections describe
the available flags, modes, and common usage patterns.

## Command-Line Flags

The following table describes every flag accepted by pg-healthcheck:

| Flag | Default | Description |
|---|---|---|
| `--host` | `localhost` | PostgreSQL host to connect to |
| `--port` | `5432` | PostgreSQL port |
| `--dbname` | `postgres` | Target database name |
| `--user` | `postgres` | Login role name (or set `PGUSER`) |
| `--password` | _(empty)_ | Password (prefer `PGPASSWORD` env var) |
| `--config` | _(none)_ | Path to a `healthcheck.yaml` config file |
| `--mode` | `single` | Operating mode: `single` or `cluster` |
| `--nodes` | _(none)_ | Comma-separated `host:port` list for cluster mode |
| `--output` | `text` | Output format: `text` or `json` |
| `--groups` | _(all)_ | Comma-separated list of groups to run, e.g. `G01,G05` |
| `--target-version` | _(current)_ | Target PostgreSQL major version for G10 checks |
| `--backrest-conf` | _(from yaml)_ | Path to `pgbackrest.conf` |
| `--no-color` | `false` | Disable ANSI color output |
| `--verbose` | `false` | Show OK findings in addition to WARN and CRITICAL |

## Single-Node Mode

Single-node mode connects to one PostgreSQL instance and runs all
enabled check groups sequentially. Use the following command as a
starting point:

```bash
./pg-healthcheck --host db1.example.com --dbname mydb --user postgres
```

To run only a subset of check groups:

```bash
./pg-healthcheck --groups G01,G05,G09 --verbose
```

To load a custom threshold configuration:

```bash
./pg-healthcheck --config /etc/pg-healthcheck/prod.yaml
```

## Cluster Mode

Cluster mode connects to every node in a pgEdge Spock cluster and
runs all check groups on each node. G12 Spock checks additionally
perform cross-node comparisons such as row-count parity and LSN lag
between node pairs. Use the following command to run cluster mode:

```bash
./pg-healthcheck \
  --mode cluster \
  --nodes node1:5432,node2:5432,node3:5432 \
  --dbname mydb \
  --user postgres
```

Each finding in cluster mode includes a `node_name` field identifying
which node produced the result. In JSON output mode, this field is
present in every finding object.

## JSON Output

JSON output mode writes a single structured document to standard
output. Use this mode to feed results into monitoring pipelines,
dashboards, or custom alerting systems:

```bash
./pg-healthcheck --output json | jq '.summary'
```

The document includes a `summary` object with counts by severity and
a `checks` array containing every finding. See the
[JSON Output](json_output.md) document for the full schema.

## Exit Codes

pg-healthcheck uses exit codes consistently so that it integrates with
scripts and CI systems:

- Exit code `0` means all checks returned OK or INFO.
- Exit code `1` means at least one check returned WARN.
- Exit code `2` means at least one check returned CRITICAL.

The following shell snippet demonstrates using the exit code in a
monitoring script:

```bash
./pg-healthcheck --host prod-db
case $? in
  0) echo "all healthy" ;;
  1) echo "warnings detected" ;;
  2) echo "critical issues detected - paging on-call" ;;
esac
```

## Running Specific Groups

To focus on one area during an incident, pass a comma-separated list
of group IDs to `--groups`. The following command runs only the
replication and WAL-related groups:

```bash
./pg-healthcheck --groups G09,G14,G15 --verbose
```

## Disabling Color

Use `--no-color` for log aggregation systems or terminals that do not
support ANSI escape codes:

```bash
./pg-healthcheck --no-color 2>&1 | tee /var/log/pg-healthcheck.log
```

## Next Steps

- The [Check Groups Reference](check_reference.md) document describes
  every check group and lists individual checks by ID.
- The [Configuration Reference](configuration.md) document explains
  how to tune thresholds for your workload.
