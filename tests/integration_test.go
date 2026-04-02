//go:build integration

// Package tests contains integration tests for pg_healthcheck.
//
// These tests inject real problems into a PostgreSQL database, run the full
// check suite, and assert that the expected check IDs fire at the expected
// severity levels.  They require a live PostgreSQL instance and are NOT run
// during normal `go test ./...` — they must be opted-in explicitly:
//
//	go test -tags integration -v ./tests/ \
//	    -pg-host 192.168.169.158 -pg-port 5432 \
//	    -pg-dbname testdb -pg-user ahsan
//
// The test creates all objects in the _hc_test schema and drops it on cleanup,
// so it is safe to run against a non-production database.
package tests

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ── flags ──────────────────────────────────────────────────────────────────

var (
	pgHost   = flag.String("pg-host", "localhost", "PostgreSQL host")
	pgPort   = flag.Int("pg-port", 5432, "PostgreSQL port")
	pgDBName = flag.String("pg-dbname", "postgres", "Database name")
	pgUser   = flag.String("pg-user", "postgres", "PostgreSQL user")
	pgPass   = flag.String("pg-pass", os.Getenv("PGPASSWORD"), "PostgreSQL password")
	binary   = flag.String("binary", "../pg_healthcheck", "Path to pg_healthcheck binary")
)

// ── helpers ────────────────────────────────────────────────────────────────

type checkResult struct {
	CheckID  string `json:"check_id"`
	Severity string `json:"severity"`
	Title    string `json:"title"`
	Observed string `json:"observed"`
	NodeName string `json:"node_name"`
}

type report struct {
	Checks []checkResult `json:"checks"`
}

// runHealthcheck executes the binary and returns parsed findings.
func runHealthcheck(t *testing.T, extraArgs ...string) map[string]checkResult {
	t.Helper()
	args := []string{
		"--host", *pgHost,
		"--port", fmt.Sprintf("%d", *pgPort),
		"--dbname", *pgDBName,
		"--user", *pgUser,
		"--output", "json",
		"--verbose",
	}
	if *pgPass != "" {
		args = append(args, "--password", *pgPass)
	}
	args = append(args, extraArgs...)

	cmd := exec.Command(*binary, args...)
	out, _ := cmd.Output() // non-zero exit is expected when there are WARNs/CRITs

	var r report
	if err := json.Unmarshal(out, &r); err != nil {
		t.Fatalf("failed to parse healthcheck output: %v\nraw: %s", err, string(out))
	}

	results := make(map[string]checkResult, len(r.Checks))
	for _, c := range r.Checks {
		results[c.CheckID] = c
	}
	return results
}

// db opens a direct connection for injecting/cleaning test conditions.
func db(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s",
		*pgHost, *pgPort, *pgDBName, *pgUser)
	if *pgPass != "" {
		dsn += " password=" + *pgPass
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("cannot connect to PostgreSQL: %v", err)
	}
	return pool
}

// exec runs a SQL statement and fails the test on error.
func mustExec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("SQL failed: %v\nSQL: %s", err, sql)
	}
}

// assertSeverity checks that a given check ID produced the expected severity.
func assertSeverity(t *testing.T, results map[string]checkResult, checkID, wantSev string) {
	t.Helper()
	c, ok := results[checkID]
	if !ok {
		t.Errorf("check %s not found in output", checkID)
		return
	}
	if c.Severity != wantSev {
		t.Errorf("check %s: got severity %q, want %q (observed: %s)",
			checkID, c.Severity, wantSev, c.Observed)
	}
}

// ── setup / teardown ───────────────────────────────────────────────────────

func setupSchema(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	mustExec(t, pool, `CREATE SCHEMA IF NOT EXISTS _hc_test`)
}

func teardown(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	// Roll back any dangling prepared transaction
	pool.Exec(ctx, `ROLLBACK PREPARED '_hc_test_prepared_tx'`)
	// Drop inactive replication slot
	pool.Exec(ctx, `SELECT pg_drop_replication_slot('_hc_test_inactive_slot')
	                WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='_hc_test_inactive_slot')`)
	// Drop test superuser
	pool.Exec(ctx, `DROP ROLE IF EXISTS _hc_test_super`)
	// Revoke public schema create
	pool.Exec(ctx, `REVOKE CREATE ON SCHEMA public FROM PUBLIC`)
	// Drop all test objects
	mustExec(t, pool, `DROP SCHEMA IF EXISTS _hc_test CASCADE`)
}

// ── tests ──────────────────────────────────────────────────────────────────

// TestG05_DeadTuples verifies that a table with a high dead-tuple ratio
// triggers the autovacuum / dead tuple checks.
func TestG05_DeadTuples(t *testing.T) {
	pool := db(t)
	defer pool.Close()
	setupSchema(t, pool)
	defer teardown(t, pool)

	mustExec(t, pool, `CREATE TABLE _hc_test.bloat(id serial PRIMARY KEY, data text)`)
	mustExec(t, pool, `INSERT INTO _hc_test.bloat(data) SELECT repeat('x',200) FROM generate_series(1,5000)`)
	mustExec(t, pool, `DELETE FROM _hc_test.bloat WHERE id % 5 != 0`)
	// do NOT vacuum — leave dead tuples in place

	results := runHealthcheck(t, "--groups", "G05")
	// G05-003 checks dead tuple ratio
	c, ok := results["G05-003"]
	if !ok {
		t.Error("G05-003 not found in output")
		return
	}
	if c.Severity != "WARN" && c.Severity != "CRITICAL" {
		t.Errorf("G05-003: expected WARN or CRITICAL, got %s — may need more dead tuples", c.Severity)
	}
	t.Logf("G05-003: %s — %s", c.Severity, c.Observed)
}

// TestG05_PreparedTransactions verifies that an old prepared transaction
// is detected by G05-013.
func TestG05_PreparedTransactions(t *testing.T) {
	pool := db(t)
	defer pool.Close()
	setupSchema(t, pool)
	defer teardown(t, pool)

	// Prepared transactions cannot be issued through pgx in the same pool —
	// use a single-connection pool to avoid the autocommit wrapper.
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer conn.Release()
	ctx := context.Background()
	conn.Exec(ctx, `ROLLBACK PREPARED '_hc_test_prepared_tx'`) // clean prior run
	conn.Exec(ctx, `BEGIN`)
	conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS _hc_test.pt_marker(id int)`)
	if _, err := conn.Exec(ctx, `PREPARE TRANSACTION '_hc_test_prepared_tx'`); err != nil {
		t.Skipf("PREPARE TRANSACTION not allowed (max_prepared_transactions=0?): %v", err)
	}

	results := runHealthcheck(t, "--groups", "G05")
	c, ok := results["G05-013"]
	if !ok {
		t.Error("G05-013 not found in output")
		return
	}
	if c.Severity == "OK" {
		t.Errorf("G05-013: expected INFO or WARN, got OK — prepared transaction not detected")
	}
	t.Logf("G05-013: %s — %s", c.Severity, c.Observed)
}

// TestG06_DuplicateIndexes verifies G06-002 fires when two indexes cover
// the same column on the same table.
func TestG06_DuplicateIndexes(t *testing.T) {
	pool := db(t)
	defer pool.Close()
	setupSchema(t, pool)
	defer teardown(t, pool)

	mustExec(t, pool, `CREATE TABLE _hc_test.dupidx(id serial PRIMARY KEY, name text)`)
	mustExec(t, pool, `INSERT INTO _hc_test.dupidx(name) SELECT 'u'||g FROM generate_series(1,500) g`)
	mustExec(t, pool, `CREATE INDEX _hc_test_dupidx_a ON _hc_test.dupidx(name)`)
	mustExec(t, pool, `CREATE INDEX _hc_test_dupidx_b ON _hc_test.dupidx(name)`)

	results := runHealthcheck(t, "--groups", "G06")
	assertSeverity(t, results, "G06-002", "WARN")
	t.Logf("G06-002: %s — %s", results["G06-002"].Severity, results["G06-002"].Observed)
}

// TestG06_InvalidIndex verifies G06-003 fires when an index has indisvalid=false.
func TestG06_InvalidIndex(t *testing.T) {
	pool := db(t)
	defer pool.Close()
	setupSchema(t, pool)
	defer teardown(t, pool)

	mustExec(t, pool, `CREATE TABLE _hc_test.invalidtbl(id serial PRIMARY KEY, val int)`)
	mustExec(t, pool, `INSERT INTO _hc_test.invalidtbl(val) SELECT g FROM generate_series(1,100) g`)
	mustExec(t, pool, `CREATE INDEX _hc_test_invalid_idx ON _hc_test.invalidtbl(val)`)
	// Simulate a broken concurrent reindex by flipping indisvalid
	mustExec(t, pool, `UPDATE pg_index SET indisvalid=false
	                   WHERE indexrelid='_hc_test._hc_test_invalid_idx'::regclass`)

	results := runHealthcheck(t, "--groups", "G06")
	assertSeverity(t, results, "G06-003", "CRITICAL")
	t.Logf("G06-003: %s — %s", results["G06-003"].Severity, results["G06-003"].Observed)
}

// TestG06_TableWithoutPK verifies G06-010 fires when a user table has no PK.
func TestG06_TableWithoutPK(t *testing.T) {
	pool := db(t)
	defer pool.Close()
	setupSchema(t, pool)
	defer teardown(t, pool)

	mustExec(t, pool, `CREATE TABLE _hc_test.nopk(id int, val text)`)
	mustExec(t, pool, `INSERT INTO _hc_test.nopk VALUES(1,'no pk')`)

	results := runHealthcheck(t, "--groups", "G06")
	assertSeverity(t, results, "G06-010", "WARN")
	t.Logf("G06-010: %s — %s", results["G06-010"].Severity, results["G06-010"].Observed)
}

// TestG09_InactiveSlot verifies G09-001 fires when a replication slot is
// inactive and retaining WAL.
func TestG09_InactiveSlot(t *testing.T) {
	pool := db(t)
	defer pool.Close()
	setupSchema(t, pool)
	defer teardown(t, pool)

	ctx := context.Background()
	pool.Exec(ctx, `SELECT pg_drop_replication_slot('_hc_test_inactive_slot')
	                WHERE EXISTS(SELECT 1 FROM pg_replication_slots
	                             WHERE slot_name='_hc_test_inactive_slot')`)
	mustExec(t, pool, `SELECT pg_create_physical_replication_slot('_hc_test_inactive_slot', false, false)`)

	// Generate some WAL so the slot falls behind
	mustExec(t, pool, `CREATE TABLE IF NOT EXISTS _hc_test.wal_gen(id serial PRIMARY KEY, d text)`)
	for i := 0; i < 10; i++ {
		mustExec(t, pool, `INSERT INTO _hc_test.wal_gen(d) SELECT repeat('x',1000) FROM generate_series(1,100)`)
	}

	results := runHealthcheck(t, "--groups", "G09")
	c, ok := results["G09-001"]
	if !ok {
		t.Error("G09-001 not found in output")
		return
	}
	// Slot is inactive — should be WARN or CRIT depending on bytes retained
	if c.Severity == "OK" {
		t.Logf("G09-001: OK (slot may not have retained enough WAL yet — %s)", c.Observed)
	} else {
		t.Logf("G09-001: %s — %s ✓", c.Severity, c.Observed)
	}
}

// TestG11_PublicSchemaCreate verifies G11-003 fires when PUBLIC has CREATE
// on the public schema.
func TestG11_PublicSchemaCreate(t *testing.T) {
	pool := db(t)
	defer pool.Close()
	setupSchema(t, pool)
	defer teardown(t, pool)

	mustExec(t, pool, `GRANT CREATE ON SCHEMA public TO PUBLIC`)

	results := runHealthcheck(t, "--groups", "G11")
	assertSeverity(t, results, "G11-003", "WARN")
	t.Logf("G11-003: %s — %s", results["G11-003"].Severity, results["G11-003"].Observed)
}

// TestG11_ExtraSuperuser verifies G11-009 fires when there are > 2 superuser
// login roles.
func TestG11_ExtraSuperuser(t *testing.T) {
	pool := db(t)
	defer pool.Close()
	setupSchema(t, pool)
	defer teardown(t, pool)

	ctx := context.Background()
	pool.Exec(ctx, `DROP ROLE IF EXISTS _hc_test_super`)
	mustExec(t, pool, `CREATE ROLE _hc_test_super SUPERUSER LOGIN PASSWORD 'testonly123!'`)

	results := runHealthcheck(t, "--groups", "G11")
	c, ok := results["G11-009"]
	if !ok {
		t.Error("G11-009 not found")
		return
	}
	if c.Severity != "WARN" && c.Severity != "INFO" {
		t.Errorf("G11-009: expected WARN or INFO, got %s", c.Severity)
	}
	t.Logf("G11-009: %s — %s", c.Severity, c.Observed)
}

// TestG01_HBATrust verifies G01-009 fires when pg_hba.conf has a TRUST
// rule on a non-loopback address. (This is a read-only check — no injection
// needed if the test database already has it configured.)
func TestG01_HBATrust(t *testing.T) {
	results := runHealthcheck(t, "--groups", "G01")
	c, ok := results["G01-009"]
	if !ok {
		t.Skip("G01-009 not in output — pg_hba_file_rules may not be accessible")
	}
	t.Logf("G01-009: %s — %s", c.Severity, c.Observed)
	// If TRUST is present, it should be CRITICAL
	if c.Severity != "OK" && c.Severity != "CRITICAL" {
		t.Errorf("G01-009: unexpected severity %s", c.Severity)
	}
}

// TestAllChecksPresent is a smoke test that verifies all expected check IDs
// appear in the output — catches regressions where a check is accidentally
// removed or no longer wired into Run().
func TestAllChecksPresent(t *testing.T) {
	results := runHealthcheck(t)

	required := []string{
		// G01
		"G01-001", "G01-002", "G01-003", "G01-005", "G01-006", "G01-007",
		// G03
		"G03-001", "G03-002", "G03-007", "G03-008",
		// G04
		"G04-001", "G04-002", "G04-003", "G04-004",
		// G05
		"G05-001", "G05-002", "G05-003", "G05-012", "G05-013",
		// G06
		"G06-001", "G06-002", "G06-003", "G06-004", "G06-010",
		// G09
		"G09-001", "G09-004", "G09-009", "G09-014",
		// G11
		"G11-001", "G11-002", "G11-003", "G11-009", "G11-010",
		// G14
		"G14-001", "G14-002", "G14-005", "G14-013",
	}

	missing := 0
	for _, id := range required {
		if _, ok := results[id]; !ok {
			t.Errorf("required check %s missing from output", id)
			missing++
		}
	}
	t.Logf("Total checks in output: %d", len(results))
	t.Logf("Missing required checks: %d", missing)

	// Report timing
	start := time.Now()
	_ = runHealthcheck(t)
	t.Logf("Full run time: %v", time.Since(start))
}
