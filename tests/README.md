# pg_healthcheck Integration Tests

## The problem this solves

Unit tests on a health check tool are weak — the only meaningful test is
whether the check fires correctly when the problem actually exists in a real
database. This framework injects known-bad conditions, runs the full binary,
and asserts the right check IDs fire at the right severity.

## Quick start (manual)

**Step 1 — inject problems into your test database:**
```bash
psql -h 192.168.169.158 -p 5432 -U ahsan -d testdb -f testdata/inject_problems.sql
```

**Step 2 — run the healthcheck and capture results:**
```bash
cd ..
./pg_healthcheck --host 192.168.169.158 --port 5432 --dbname testdb --user ahsan \
    --output json --verbose > /tmp/hc_results.json
```

**Step 3 — check what fired:**
```bash
python3 -c "
import json
data = json.load(open('/tmp/hc_results.json'))
for c in data['checks']:
    if c['severity'] in ('WARN','CRITICAL'):
        print(f\"{c['check_id']:10} [{c['severity']:8}] {c['observed'][:70]}\")
"
```

**Step 4 — clean up:**
```bash
psql -h 192.168.169.158 -p 5432 -U ahsan -d testdb -f testdata/cleanup.sql
```

## Automated Go integration tests

The integration tests require a real PostgreSQL instance. They are opt-in
(not run by `go test ./...`) and use the `-tags integration` build tag.

```bash
# Build the binary first
cd ..
go build -o pg_healthcheck ./cmd/pg_healthcheck/

# Run all integration tests
go test -tags integration -v ./tests/ \
    -pg-host 192.168.169.158 \
    -pg-port 5432 \
    -pg-dbname testdb \
    -pg-user ahsan

# Run a single test
go test -tags integration -v ./tests/ \
    -pg-host 192.168.169.158 -pg-port 5432 -pg-dbname testdb -pg-user ahsan \
    -run TestG06_InvalidIndex
```

Each test:
1. Creates objects in the `_hc_test` schema
2. Runs the healthcheck binary
3. Asserts the expected check ID fires at the expected severity
4. Drops the `_hc_test` schema on cleanup

## What each test covers

| Test | Check ID | Condition injected |
|------|----------|--------------------|
| `TestG05_DeadTuples` | G05-003 | 5000 rows inserted, 80% deleted, no VACUUM |
| `TestG05_PreparedTransactions` | G05-013 | `PREPARE TRANSACTION` left uncommitted |
| `TestG06_DuplicateIndexes` | G06-002 | Two indexes on the same column |
| `TestG06_InvalidIndex` | G06-003 | Index with `indisvalid=false` |
| `TestG06_TableWithoutPK` | G06-010 | Table with no primary key |
| `TestG09_InactiveSlot` | G09-001 | Physical replication slot, never consumed |
| `TestG11_PublicSchemaCreate` | G11-003 | `GRANT CREATE ON SCHEMA public TO PUBLIC` |
| `TestG11_ExtraSuperuser` | G11-009 | Extra SUPERUSER LOGIN role created |
| `TestG01_HBATrust` | G01-009 | Reads live pg_hba.conf (no injection needed) |
| `TestAllChecksPresent` | all | Smoke test — every expected check ID appears |

## Lock blocker test (manual only)

The lock blocker check (G04-003) requires two concurrent sessions and cannot
be automated in a single script. See `testdata/inject_lock_blocker.sql` for
step-by-step instructions.

## Adding a new test

1. Add a function `TestGXX_Description(t *testing.T)` to `integration_test.go`
2. Use `mustExec` to inject the bad condition
3. Call `runHealthcheck(t, "--groups", "GXX")`
4. Call `assertSeverity(t, results, "GXX-NNN", "WARN")` (or CRITICAL/INFO)
5. Add the check ID to `TestAllChecksPresent`'s `required` list
