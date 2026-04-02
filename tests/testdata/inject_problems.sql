-- ============================================================
-- pg_healthcheck — Problem Injection Script
--
-- Purpose: Creates deliberate bad conditions in the database
--          so every check can be tested against a real problem,
--          not just a clean baseline.
--
-- Usage:
--   psql -h HOST -U USER -d DB -f inject_problems.sql
--   ./pg_healthcheck --host HOST --dbname DB --user USER --output json > results.json
--   psql -h HOST -U USER -d DB -f cleanup.sql
--
-- Each block is labelled with the check ID it is designed to trigger.
-- IMPORTANT: Run cleanup.sql after every test run.
-- ============================================================

\set ON_ERROR_STOP on
\echo '=== pg_healthcheck problem injection starting ==='

-- ── Schema for all test objects ──────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS _hc_test;

-- ── G05-003 / G05-004  Dead tuples (bloat) ───────────────────────────────────
-- Creates a table with lots of dead tuples that autovacuum hasn't yet reclaimed.
\echo '--- Injecting: G05 dead tuples ---'
CREATE TABLE IF NOT EXISTS _hc_test.bloat_source (
    id   serial PRIMARY KEY,
    data text DEFAULT repeat('x', 200)
);
INSERT INTO _hc_test.bloat_source (data)
SELECT repeat('row-' || g::text, 40)
FROM generate_series(1, 5000) g;
-- Delete 80% of rows to create dead tuples — do NOT run VACUUM after this
DELETE FROM _hc_test.bloat_source WHERE id % 5 != 0;

-- ── G05-012  Multixact (row-lock) activity ───────────────────────────────────
-- Hard to inject a high mxid_age without waiting, so we just verify the check
-- runs correctly. The check itself reads from pg_database.

-- ── G05-013  Prepared transactions ───────────────────────────────────────────
-- Creates a prepared transaction that will appear in pg_prepared_xacts.
-- NOTE: Requires max_prepared_transactions > 0 (default is 0 on many systems).
--       If this block is skipped, set max_prepared_transactions >= 10 and reload.
\echo '--- Injecting: G05-013 prepared transaction ---'
SELECT current_setting('max_prepared_transactions')::int > 0 AS prepared_tx_ok \gset
\if :prepared_tx_ok
BEGIN;
CREATE TABLE IF NOT EXISTS _hc_test.prepared_tx_marker (id int);
PREPARE TRANSACTION '_hc_test_prepared_tx';
-- This transaction now sits in pg_prepared_xacts until cleanup.sql rolls it back.
\else
\echo 'SKIP G05-013: max_prepared_transactions=0 — run: ALTER SYSTEM SET max_prepared_transactions=10; SELECT pg_reload_conf();'
\endif

-- ── G06-002  Duplicate indexes ────────────────────────────────────────────────
\echo '--- Injecting: G06-002 duplicate indexes ---'
CREATE TABLE IF NOT EXISTS _hc_test.dup_idx_table (
    id   serial PRIMARY KEY,
    name text
);
INSERT INTO _hc_test.dup_idx_table (name)
SELECT 'user_' || g FROM generate_series(1, 1000) g;
CREATE INDEX IF NOT EXISTS _hc_test_dup_idx_a ON _hc_test.dup_idx_table (name);
CREATE INDEX IF NOT EXISTS _hc_test_dup_idx_b ON _hc_test.dup_idx_table (name);  -- exact duplicate

-- ── G06-003  Invalid index ────────────────────────────────────────────────────
-- Mark an existing index as invalid by directly updating pg_index.
-- (The clean way is to let a REINDEX CONCURRENTLY be interrupted, but that is
--  not reliable in automation. Instead we create a valid index and then flip
--  indisvalid=false to simulate the post-crash state.)
\echo '--- Injecting: G06-003 invalid index ---'
CREATE TABLE IF NOT EXISTS _hc_test.invalid_idx_table (id serial PRIMARY KEY, val int);
INSERT INTO _hc_test.invalid_idx_table (val) SELECT g FROM generate_series(1,100) g;
CREATE INDEX IF NOT EXISTS _hc_test_invalid_idx ON _hc_test.invalid_idx_table (val);
UPDATE pg_index SET indisvalid = false
WHERE indexrelid = '_hc_test._hc_test_invalid_idx'::regclass;

-- ── G06-005  FK without supporting index ─────────────────────────────────────
\echo '--- Injecting: G06-005 FK without index ---'
CREATE TABLE IF NOT EXISTS _hc_test.fk_parent (id serial PRIMARY KEY);
CREATE TABLE IF NOT EXISTS _hc_test.fk_child (
    id        serial PRIMARY KEY,
    parent_id int REFERENCES _hc_test.fk_parent(id)
    -- Deliberately no index on parent_id
);
INSERT INTO _hc_test.fk_parent DEFAULT VALUES;
INSERT INTO _hc_test.fk_child (parent_id) VALUES (1);

-- ── G06-010  Table without primary key ────────────────────────────────────────
\echo '--- Injecting: G06-010 table without PK ---'
CREATE TABLE IF NOT EXISTS _hc_test.no_pk_table (
    id   int,
    name text
);
INSERT INTO _hc_test.no_pk_table VALUES (1, 'no primary key here');

-- ── G09-001  Inactive replication slot retaining WAL ─────────────────────────
\echo '--- Injecting: G09-001 inactive replication slot ---'
SELECT pg_create_physical_replication_slot('_hc_test_inactive_slot', false, false)
WHERE NOT EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name = '_hc_test_inactive_slot'
);
-- Slot is created but never consumed — it will retain WAL from this point forward.

-- ── G09-010  max_slot_wal_keep_size = -1 (unlimited) ─────────────────────────
-- This is a GUC setting — cannot be injected via SQL without superuser ALTER SYSTEM.
-- The check reads the current value; if already -1 it will WARN automatically.

-- ── G11-003  Public schema CREATE privilege ────────────────────────────────────
\echo '--- Injecting: G11-003 public schema CREATE ---'
GRANT CREATE ON SCHEMA public TO PUBLIC;

-- ── G11-009  Extra superuser login role ────────────────────────────────────────
\echo '--- Injecting: G11-009 extra superuser ---'
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '_hc_test_super') THEN
        CREATE ROLE _hc_test_super SUPERUSER LOGIN PASSWORD 'testonly123';
    END IF;
END$$;

-- ── G04-003  Lock blocking chain ──────────────────────────────────────────────
-- Cannot be injected in a single SQL script (requires two concurrent sessions).
-- Use the companion script tests/testdata/inject_lock_blocker.sql in two terminals.

-- ── G12-014  Sequence with increment=1 on cluster ─────────────────────────────
\echo '--- Injecting: G12-014 sequence with increment=1 ---'
CREATE SEQUENCE IF NOT EXISTS _hc_test.risky_seq INCREMENT 1 START 1;

\echo '=== Problem injection complete ==='
\echo 'Run: ./pg_healthcheck --host HOST --dbname DB --user USER --output json'
\echo 'Then: psql -f cleanup.sql to restore clean state'
