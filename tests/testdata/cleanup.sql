-- ============================================================
-- pg_healthcheck — Problem Cleanup Script
--
-- Reverses everything inject_problems.sql created.
-- Safe to run multiple times (all statements are IF EXISTS).
-- ============================================================

\echo '=== pg_healthcheck cleanup starting ==='

-- Roll back the prepared transaction first (must be done before dropping the schema)
\echo '--- Rolling back prepared transaction ---'
ROLLBACK PREPARED '_hc_test_prepared_tx';

-- Drop the inactive replication slot
\echo '--- Dropping test replication slot ---'
SELECT pg_drop_replication_slot('_hc_test_inactive_slot')
WHERE EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name = '_hc_test_inactive_slot'
);

-- Revoke the public schema privilege
\echo '--- Revoking public schema CREATE ---'
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

-- Drop the test superuser
\echo '--- Dropping test superuser role ---'
DROP ROLE IF EXISTS _hc_test_super;

-- Drop all test tables and sequences (dropping the schema cascades everything)
\echo '--- Dropping _hc_test schema ---'
DROP SCHEMA IF EXISTS _hc_test CASCADE;

\echo '=== Cleanup complete — database restored to pre-test state ==='
