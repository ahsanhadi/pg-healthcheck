package checks

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/pg_healthcheck/internal/config"
)

const g10 = "pg_upgrade Readiness"

// G10Upgrade checks readiness for a major version upgrade.
type G10Upgrade struct{}

func (g *G10Upgrade) Name() string    { return g10 }
func (g *G10Upgrade) GroupID() string { return "G10" }

func (g *G10Upgrade) Run(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) ([]Finding, error) {
	if cfg.TargetVersion == 0 {
		return []Finding{NewInfo("G10-000", g10, "pg_upgrade readiness",
			"target_version not configured",
			"Set --target-version (e.g. 17) to enable pg_upgrade readiness checks.",
			"",
			"https://www.postgresql.org/docs/current/pgupgrade.html")}, nil
	}

	var f []Finding
	f = append(f, g10LegacyExtensions(ctx, db, cfg)...)
	f = append(f, g10PostGIS(ctx, db)...)
	f = append(f, g10AbsTimeColumns(ctx, db, cfg)...)
	f = append(f, g10MoneyColumns(ctx, db)...)
	f = append(f, g10SQLASCIIDatabases(ctx, db)...)
	f = append(f, g10CollationVersion(ctx, db)...)
	f = append(f, g10WideColumns(ctx, db)...)
	f = append(f, g10PreparedTransactions(ctx, db)...)
	f = append(f, g10LogicalSlotsOldPG(ctx, db, cfg)...)
	f = append(f, g10CLangFunctions(ctx, db)...)
	f = append(f, g10Plpython2u(ctx, db, cfg)...)
	f = append(f, g10GhostExtensions(ctx, db)...)
	f = append(f, g10LargeObjects(ctx, db)...)
	f = append(f, g10CustomTablespaces(ctx, db)...)
	f = append(f, g10ZeroConnLimitDatabases(ctx, db)...)
	return f, nil
}

// G10-001 plpython2u / tsearch2 installed and target >= 14
func g10LegacyExtensions(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) []Finding {
	if cfg.TargetVersion < 14 {
		return []Finding{NewSkip("G10-001", g10, "Legacy extensions (tsearch2/plpython2u)",
			fmt.Sprintf("target version %d < 14; not applicable", cfg.TargetVersion))}
	}
	const q = `SELECT extname FROM pg_extension WHERE extname IN ('tsearch2','plpython2u') ORDER BY 1`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G10-001", g10, "Legacy extensions (tsearch2/plpython2u)", err.Error())}
	}
	defer rows.Close()
	var found []string
	for rows.Next() {
		var name string
		_ = rows.Scan(&name)
		found = append(found, name)
	}
	if err := rows.Err(); err != nil {
		return []Finding{NewSkip("G10-001", g10, "Legacy extensions (tsearch2/plpython2u)", "scan error: "+err.Error())}
	}
	if len(found) > 0 {
		return []Finding{NewCrit("G10-001", g10, "Legacy extensions (tsearch2/plpython2u)",
			fmt.Sprintf("Found: %s — incompatible with PostgreSQL %d", strings.Join(found, ", "), cfg.TargetVersion),
			"Remove these extensions before upgrading; tsearch2 and plpython2u are removed in PG14.",
			"",
			"https://www.postgresql.org/docs/current/pgupgrade.html")}
	}
	return []Finding{NewOK("G10-001", g10, "Legacy extensions (tsearch2/plpython2u)",
		"No legacy extensions found",
		"https://www.postgresql.org/docs/current/pgupgrade.html")}
}

// G10-002 PostGIS version info
func g10PostGIS(ctx context.Context, db *pgxpool.Pool) []Finding {
	var installed bool
	if err := db.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='postgis')").Scan(&installed); err != nil || !installed {
		return []Finding{NewOK("G10-002", g10, "PostGIS version check",
			"PostGIS is not installed",
			"https://postgis.net/documentation/")}
	}
	var version string
	_ = db.QueryRow(ctx, "SELECT extversion FROM pg_extension WHERE extname='postgis'").Scan(&version)
	return []Finding{NewInfo("G10-002", g10, "PostGIS version check",
		fmt.Sprintf("PostGIS %s is installed", version),
		"Verify PostGIS compatibility with your target PostgreSQL version before upgrading.",
		"PostGIS upgrades often require a separate upgrade step after pg_upgrade.",
		"https://postgis.net/docs/postgis_installation.html#upgrading")}
}

// G10-003 abstime/reltime/tinterval columns if target >= 12
func g10AbsTimeColumns(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) []Finding {
	if cfg.TargetVersion < 12 {
		return []Finding{NewSkip("G10-003", g10, "abstime/reltime/tinterval columns",
			fmt.Sprintf("target version %d < 12; not applicable", cfg.TargetVersion))}
	}
	const q = `SELECT n.nspname || '.' || c.relname || '.' || a.attname AS col, t.typname
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_type t ON t.oid = a.atttypid
		WHERE t.typname IN ('abstime','reltime','tinterval')
		AND c.relkind = 'r'
		AND NOT a.attisdropped
		ORDER BY 1`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G10-003", g10, "abstime/reltime/tinterval columns", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var col, typname string
		_ = rows.Scan(&col, &typname)
		lines = append(lines, fmt.Sprintf("%s (%s)", col, typname))
	}
	if err := rows.Err(); err != nil {
		return []Finding{NewSkip("G10-003", g10, "abstime/reltime/tinterval columns", "scan error: "+err.Error())}
	}
	if len(lines) > 0 {
		return []Finding{NewCrit("G10-003", g10, "abstime/reltime/tinterval columns",
			fmt.Sprintf("%d column(s) use removed types", len(lines)),
			"Convert these columns to timestamp/interval before upgrading to PG12+.",
			strings.Join(lines, "\n"),
			"https://www.postgresql.org/docs/12/release-12.html#id-1.11.6.10.4")}
	}
	return []Finding{NewOK("G10-003", g10, "abstime/reltime/tinterval columns",
		"No abstime/reltime/tinterval columns found",
		"https://www.postgresql.org/docs/current/pgupgrade.html")}
}

// G10-004 money type columns
func g10MoneyColumns(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT count(*) FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_type t ON t.oid = a.atttypid
		WHERE t.typname = 'money' AND c.relkind = 'r' AND NOT a.attisdropped`
	var cnt int
	if err := db.QueryRow(ctx, q).Scan(&cnt); err != nil {
		return []Finding{NewSkip("G10-004", g10, "money type columns", err.Error())}
	}
	if cnt > 0 {
		return []Finding{NewInfo("G10-004", g10, "money type columns",
			fmt.Sprintf("%d column(s) use the money type", cnt),
			"Verify lc_monetary is identical between source and target clusters before upgrading.",
			"money type output depends on lc_monetary; mismatches cause data corruption on upgrade.",
			"https://www.postgresql.org/docs/current/datatype-money.html")}
	}
	return []Finding{NewOK("G10-004", g10, "money type columns",
		"No money type columns found",
		"https://www.postgresql.org/docs/current/pgupgrade.html")}
}

// G10-005 SQL_ASCII databases
func g10SQLASCIIDatabases(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT datname FROM pg_database WHERE pg_encoding_to_char(encoding)='SQL_ASCII' AND datname NOT IN ('template0')`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G10-005", g10, "SQL_ASCII databases", err.Error())}
	}
	defer rows.Close()
	var found []string
	for rows.Next() {
		var name string
		_ = rows.Scan(&name)
		found = append(found, name)
	}
	if err := rows.Err(); err != nil {
		return []Finding{NewSkip("G10-005", g10, "SQL_ASCII databases", "scan error: "+err.Error())}
	}
	if len(found) > 0 {
		return []Finding{NewWarn("G10-005", g10, "SQL_ASCII databases",
			fmt.Sprintf("%d SQL_ASCII database(s): %s", len(found), strings.Join(found, ", ")),
			"Plan conversion to UTF-8; SQL_ASCII disables encoding validation and causes issues.",
			"",
			"https://www.postgresql.org/docs/current/multibyte.html")}
	}
	return []Finding{NewOK("G10-005", g10, "SQL_ASCII databases",
		"No SQL_ASCII databases found",
		"https://www.postgresql.org/docs/current/pgupgrade.html")}
}

// G10-006 collation version info
func g10CollationVersion(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT datname, datcollversion FROM pg_database
		WHERE datcollversion IS NOT NULL AND datname NOT IN ('template0','template1')
		ORDER BY 1`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G10-006", g10, "Collation version", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var datname string
		var collver *string
		_ = rows.Scan(&datname, &collver)
		v := "(none)"
		if collver != nil {
			v = *collver
		}
		lines = append(lines, fmt.Sprintf("%s: %s", datname, v))
	}
	if err := rows.Err(); err != nil {
		return []Finding{NewSkip("G10-006", g10, "Collation version", "scan error: "+err.Error())}
	}
	if len(lines) == 0 {
		return []Finding{NewOK("G10-006", g10, "Collation version",
			"No collation version information available",
			"https://www.postgresql.org/docs/current/collation.html")}
	}
	return []Finding{NewInfo("G10-006", g10, "Collation version",
		fmt.Sprintf("Collation versions recorded for %d database(s)", len(lines)),
		"After upgrade, run ALTER DATABASE ... REFRESH COLLATION VERSION to update collation checksums.",
		strings.Join(lines, "\n"),
		"https://www.postgresql.org/docs/current/sql-alterdatabase.html")}
}

// G10-007 tables with > 1600 columns
func g10WideColumns(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT n.nspname || '.' || c.relname AS tbl, count(*) AS ncols
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = 'r' AND NOT a.attisdropped AND a.attnum > 0
		GROUP BY 1 HAVING count(*) > 1600 ORDER BY 2 DESC`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G10-007", g10, "Tables with > 1600 columns", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var tbl string
		var ncols int
		_ = rows.Scan(&tbl, &ncols)
		lines = append(lines, fmt.Sprintf("%s: %d columns", tbl, ncols))
	}
	if err := rows.Err(); err != nil {
		return []Finding{NewSkip("G10-007", g10, "Tables with > 1600 columns", "scan error: "+err.Error())}
	}
	if len(lines) > 0 {
		return []Finding{NewCrit("G10-007", g10, "Tables with > 1600 columns",
			fmt.Sprintf("%d table(s) exceed 1600-column limit", len(lines)),
			"Split wide tables before upgrading; PostgreSQL hard-limits tables to 1600 columns.",
			strings.Join(lines, "\n"),
			"https://www.postgresql.org/docs/current/limits.html")}
	}
	return []Finding{NewOK("G10-007", g10, "Tables with > 1600 columns",
		"No tables exceed 1600 columns",
		"https://www.postgresql.org/docs/current/pgupgrade.html")}
}

// G10-008 pending prepared transactions
func g10PreparedTransactions(ctx context.Context, db *pgxpool.Pool) []Finding {
	var cnt int
	if err := db.QueryRow(ctx, "SELECT count(*) FROM pg_prepared_xacts").Scan(&cnt); err != nil {
		return []Finding{NewSkip("G10-008", g10, "Pending prepared transactions", err.Error())}
	}
	if cnt > 0 {
		return []Finding{NewCrit("G10-008", g10, "Pending prepared transactions",
			fmt.Sprintf("%d pending prepared transaction(s)", cnt),
			"COMMIT or ROLLBACK all prepared transactions before running pg_upgrade.",
			"pg_upgrade will fail with pending prepared transactions.",
			"https://www.postgresql.org/docs/current/sql-prepare-transaction.html")}
	}
	return []Finding{NewOK("G10-008", g10, "Pending prepared transactions",
		"No pending prepared transactions",
		"https://www.postgresql.org/docs/current/pgupgrade.html")}
}

// G10-009 logical slots on PG < 17
func g10LogicalSlotsOldPG(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) []Finding {
	var major int
	_ = db.QueryRow(ctx, "SELECT current_setting('server_version_num')::int / 10000").Scan(&major)
	if major >= 17 {
		return []Finding{NewOK("G10-009", g10, "Logical slots on upgrade",
			fmt.Sprintf("PostgreSQL %d supports logical slot migration via pg_upgrade", major),
			"https://www.postgresql.org/docs/current/pgupgrade.html")}
	}
	const q = `SELECT count(*) FROM pg_replication_slots WHERE slot_type='logical'`
	var cnt int
	if err := db.QueryRow(ctx, q).Scan(&cnt); err != nil {
		return []Finding{NewSkip("G10-009", g10, "Logical slots on PG < 17", err.Error())}
	}
	if cnt > 0 {
		return []Finding{NewCrit("G10-009", g10, "Logical slots on PG < 17",
			fmt.Sprintf("%d logical slot(s) found on PostgreSQL %d", cnt, major),
			"Drop all logical replication slots before running pg_upgrade on PG < 17.",
			"pg_upgrade does not preserve logical replication slots on versions prior to PG17.",
			"https://www.postgresql.org/docs/current/pgupgrade.html")}
	}
	return []Finding{NewOK("G10-009", g10, "Logical slots on PG < 17",
		"No logical replication slots to worry about",
		"https://www.postgresql.org/docs/current/pgupgrade.html")}
}

// G10-010 C-language functions
func g10CLangFunctions(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT count(*) FROM pg_proc WHERE prolang = (SELECT oid FROM pg_language WHERE lanname='c')
		AND pronamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname IN ('pg_catalog','information_schema'))`
	var cnt int
	if err := db.QueryRow(ctx, q).Scan(&cnt); err != nil {
		return []Finding{NewSkip("G10-010", g10, "C-language functions", err.Error())}
	}
	if cnt > 0 {
		return []Finding{NewWarn("G10-010", g10, "C-language functions",
			fmt.Sprintf("%d user-defined C function(s) found", cnt),
			"Recompile C extensions for the new PostgreSQL version before or after pg_upgrade.",
			"C functions are compiled against a specific major version and must be rebuilt.",
			"https://www.postgresql.org/docs/current/pgupgrade.html")}
	}
	return []Finding{NewOK("G10-010", g10, "C-language functions",
		"No user-defined C functions found",
		"https://www.postgresql.org/docs/current/pgupgrade.html")}
}

// G10-011 plpython2u on target >= 14
func g10Plpython2u(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) []Finding {
	if cfg.TargetVersion < 14 {
		return []Finding{NewSkip("G10-011", g10, "plpython2u on target >= 14",
			fmt.Sprintf("target version %d < 14", cfg.TargetVersion))}
	}
	const q = `SELECT count(*) FROM pg_proc p
		JOIN pg_language l ON l.oid = p.prolang
		WHERE l.lanname = 'plpython2u'`
	var cnt int
	if err := db.QueryRow(ctx, q).Scan(&cnt); err != nil {
		return []Finding{NewSkip("G10-011", g10, "plpython2u functions", err.Error())}
	}
	if cnt > 0 {
		return []Finding{NewCrit("G10-011", g10, "plpython2u functions",
			fmt.Sprintf("%d plpython2u function(s) found — incompatible with PG%d", cnt, cfg.TargetVersion),
			"Migrate plpython2u functions to plpython3u before upgrading.",
			"plpython2u was removed in PostgreSQL 14.",
			"https://www.postgresql.org/docs/current/plpython.html")}
	}
	return []Finding{NewOK("G10-011", g10, "plpython2u functions",
		"No plpython2u functions found",
		"https://www.postgresql.org/docs/current/pgupgrade.html")}
}

// G10-012 ghost extensions
func g10GhostExtensions(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT extname FROM pg_extension e
		WHERE NOT EXISTS (
			SELECT 1 FROM pg_available_extensions WHERE name = e.extname
		)
		ORDER BY 1`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G10-012", g10, "Ghost extensions", err.Error())}
	}
	defer rows.Close()
	var found []string
	for rows.Next() {
		var name string
		_ = rows.Scan(&name)
		found = append(found, name)
	}
	if err := rows.Err(); err != nil {
		return []Finding{NewSkip("G10-012", g10, "Ghost extensions", "scan error: "+err.Error())}
	}
	if len(found) > 0 {
		return []Finding{NewCrit("G10-012", g10, "Ghost extensions",
			fmt.Sprintf("%d ghost extension(s): %s", len(found), strings.Join(found, ", ")),
			"Install the missing extension package or DROP EXTENSION before upgrading.",
			"Ghost extensions will cause pg_upgrade --check to fail.",
			"https://www.postgresql.org/docs/current/pgupgrade.html")}
	}
	return []Finding{NewOK("G10-012", g10, "Ghost extensions",
		"No ghost extensions found",
		"https://www.postgresql.org/docs/current/pgupgrade.html")}
}

// G10-013 large objects count
func g10LargeObjects(ctx context.Context, db *pgxpool.Pool) []Finding {
	var cnt int64
	if err := db.QueryRow(ctx, "SELECT count(*) FROM pg_largeobject_metadata").Scan(&cnt); err != nil {
		return []Finding{NewSkip("G10-013", g10, "Large objects", err.Error())}
	}
	obs := fmt.Sprintf("%d large object(s)", cnt)
	if cnt > 0 {
		return []Finding{NewInfo("G10-013", g10, "Large objects", obs,
			"pg_upgrade migrates large objects, but they increase upgrade time significantly.",
			"Consider migrating large objects to file storage or object storage before upgrading.",
			"https://www.postgresql.org/docs/current/largeobjects.html")}
	}
	return []Finding{NewOK("G10-013", g10, "Large objects", obs,
		"https://www.postgresql.org/docs/current/pgupgrade.html")}
}

// G10-014 custom tablespace paths
func g10CustomTablespaces(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT spcname, pg_tablespace_location(oid) AS path
		FROM pg_tablespace
		WHERE spcname NOT IN ('pg_default','pg_global')
		ORDER BY 1`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G10-014", g10, "Custom tablespace paths", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var name, path string
		_ = rows.Scan(&name, &path)
		lines = append(lines, fmt.Sprintf("%s -> %s", name, path))
	}
	if err := rows.Err(); err != nil {
		return []Finding{NewSkip("G10-014", g10, "Custom tablespace paths", "scan error: "+err.Error())}
	}
	if len(lines) > 0 {
		return []Finding{NewWarn("G10-014", g10, "Custom tablespace paths",
			fmt.Sprintf("%d custom tablespace(s)", len(lines)),
			"Ensure tablespace paths are available and writable on the target system before pg_upgrade.",
			strings.Join(lines, "\n"),
			"https://www.postgresql.org/docs/current/manage-ag-tablespaces.html")}
	}
	return []Finding{NewOK("G10-014", g10, "Custom tablespace paths",
		"No custom tablespaces",
		"https://www.postgresql.org/docs/current/pgupgrade.html")}
}

// G10-015 datconnlimit=0 databases
func g10ZeroConnLimitDatabases(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT datname FROM pg_database
		WHERE datconnlimit = 0 AND datname NOT IN ('template0','template1')
		ORDER BY 1`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G10-015", g10, "datconnlimit=0 databases", err.Error())}
	}
	defer rows.Close()
	var found []string
	for rows.Next() {
		var name string
		_ = rows.Scan(&name)
		found = append(found, name)
	}
	if err := rows.Err(); err != nil {
		return []Finding{NewSkip("G10-015", g10, "datconnlimit=0 databases", "scan error: "+err.Error())}
	}
	if len(found) > 0 {
		return []Finding{NewWarn("G10-015", g10, "datconnlimit=0 databases",
			fmt.Sprintf("%d database(s) with datconnlimit=0: %s", len(found), strings.Join(found, ", ")),
			"Databases with datconnlimit=0 refuse all connections; verify this is intentional.",
			"pg_upgrade requires connecting to each database; datconnlimit=0 will cause it to fail.",
			"https://www.postgresql.org/docs/current/sql-createdatabase.html")}
	}
	return []Finding{NewOK("G10-015", g10, "datconnlimit=0 databases",
		"No databases with datconnlimit=0",
		"https://www.postgresql.org/docs/current/pgupgrade.html")}
}
