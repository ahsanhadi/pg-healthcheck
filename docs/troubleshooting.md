# Troubleshooting

This document covers common problems encountered when running
pg_healthcheck, along with their causes and resolutions.

## Connection Issues

This section covers errors that occur before any checks run.

### "connection refused" or "no route to host"

pg_healthcheck cannot reach the PostgreSQL host. Verify that the host
and port are correct with `--host` and `--port`. Confirm that
PostgreSQL is running and that the firewall allows connections on the
target port.

### "password authentication failed"

The supplied role or password is incorrect. Check the `--user` flag
and ensure the `PGPASSWORD` environment variable or `--password` flag
provides the correct password. Verify that `pg_hba.conf` allows the
connection method being used.

### "permission denied for view pg_stat_activity"

The connecting role does not have the `pg_monitor` built-in role.
Grant it with the following command:

```sql
GRANT pg_monitor TO your_role;
```

Some G11 security checks and G07 amcheck verification require
superuser privileges and return partial results or skip without it.

## Check-Specific Issues

This section covers issues with individual check groups.

### G02 checks all show "Skipped"

pgBackRest is not installed or the `backrest_config` path in
`healthcheck.yaml` does not point to a valid `pgbackrest.conf` file.
All G02 checks skip gracefully when pgBackRest is absent. Verify the
path with `--backrest-conf /path/to/pgbackrest.conf`.

### G07-004 and G07-008 show "Skipped - amcheck not installed"

The `amcheck` extension is not installed in the target database.
Install it with the following command:

```sql
CREATE EXTENSION amcheck;
```

Then list the tables to verify in `healthcheck.yaml` under
`amcheck_table_list`.

### G12 checks all show a single INFO and skip

The pgEdge Spock extension is not installed in the target database.
G12 is designed to skip safely on standard PostgreSQL. If Spock is
installed, verify that the connecting role can access the `spock`
schema.

### G13-010 shows "Skipped - run locally"

G13-010 uses `syscall.Statfs` to measure data directory disk space,
which requires pg_healthcheck to run on the same host as the
PostgreSQL data directory. Run the tool directly on the database host
rather than over the network.

### G14-002 and G14-003 show "Insufficient data"

The WAL rate baseline requires at least two runs separated by a short
interval. After the first run creates the state file at the path
configured in `wal_rate_state_file`, subsequent runs compare the
current rate against the rolling average. Ensure the state file path
is writable and persistent across restarts.

### G15-001 reports CRITICAL on a primary node

G15-001 skips on primary nodes (where `pg_is_in_recovery()` returns
`false`) and returns OK with an informational note. If G15-001 is
reporting CRITICAL on a node that should be a primary, verify the
recovery state with the following query:

```sql
SELECT pg_is_in_recovery();
```

## Output and Formatting

This section covers problems with pg_healthcheck output.

### ANSI color codes appear as raw characters in log files

Use `--no-color` to disable ANSI formatting when redirecting output
to a file or log aggregation system:

```bash
./pg_healthcheck --no-color 2>&1 | tee /var/log/pg_healthcheck.log
```

### JSON output is missing some fields

Fields with empty values (`""`) are omitted from JSON output. The
`recommended`, `detail`, `doc_url`, and `node_name` fields are
optional; their absence means the value is an empty string. This is
expected behavior and does not indicate a parsing error.

## Still Having Issues?

Report bugs and unexpected behavior at the
[pg_healthcheck issue tracker](https://github.com/ahsanhadi/pg_healthcheck/issues).
Include the pg_healthcheck version (`--version`), the PostgreSQL
version, and the full output with `--verbose`.
