package checks

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/pg_healthcheck/internal/config"
)

const g11 = "Security Posture"

// G11Security checks PostgreSQL security configuration.
type G11Security struct{}

func (g *G11Security) Name() string    { return g11 }
func (g *G11Security) GroupID() string { return "G11" }

func (g *G11Security) Run(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) ([]Finding, error) {
	var f []Finding
	f = append(f, g11SuperusersNoPassword(ctx, db)...)
	f = append(f, g11MD5Auth(ctx, db)...)
	f = append(f, g11PublicSchemaCreate(ctx, db)...)
	f = append(f, g11PrivilegedRoles(ctx, db)...)
	f = append(f, g11ConnectionLogging(ctx, db)...)
	f = append(f, g11PgAudit(ctx, db)...)
	f = append(f, g11StaleLoginAccounts(ctx, db)...)
	f = append(f, g11SSLCertPaths(ctx, db)...)
	return f, nil
}

// G11-001 superusers with rolcanlogin AND rolpassword IS NULL
func g11SuperusersNoPassword(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT rolname FROM pg_roles
		WHERE rolsuper = true AND rolcanlogin = true AND rolpassword IS NULL
		ORDER BY 1`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G11-001", g11, "Superusers without password", err.Error())}
	}
	defer rows.Close()
	var found []string
	for rows.Next() {
		var name string
		_ = rows.Scan(&name)
		found = append(found, name)
	}
	if len(found) > 0 {
		return []Finding{NewCrit("G11-001", g11, "Superusers without password",
			fmt.Sprintf("%d superuser(s) have no password: %s", len(found), strings.Join(found, ", ")),
			"Set passwords for all superuser login roles or restrict to peer/cert authentication.",
			"Superuser accounts without passwords can be accessed without authentication if pg_hba allows it.",
			"https://www.postgresql.org/docs/current/database-roles.html")}
	}
	return []Finding{NewOK("G11-001", g11, "Superusers without password",
		"All superuser login roles have passwords set",
		"https://www.postgresql.org/docs/current/database-roles.html")}
}

// G11-002 pg_hba_file_rules auth_method='md5'
func g11MD5Auth(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT count(*) FROM pg_hba_file_rules WHERE auth_method = 'md5'`
	var cnt int
	if err := db.QueryRow(ctx, q).Scan(&cnt); err != nil {
		return []Finding{NewSkip("G11-002", g11, "MD5 authentication entries", err.Error())}
	}
	if cnt > 0 {
		return []Finding{NewWarn("G11-002", g11, "MD5 authentication entries",
			fmt.Sprintf("%d pg_hba entry(ies) use MD5 authentication", cnt),
			"Migrate to scram-sha-256 by setting password_encryption=scram-sha-256 and resetting passwords.",
			"MD5 is a weak hashing algorithm vulnerable to offline brute-force attacks.",
			"https://www.postgresql.org/docs/current/auth-password.html")}
	}
	return []Finding{NewOK("G11-002", g11, "MD5 authentication entries",
		"No MD5 authentication entries in pg_hba.conf",
		"https://www.postgresql.org/docs/current/auth-password.html")}
}

// G11-003 has_schema_privilege('public','CREATE')
func g11PublicSchemaCreate(ctx context.Context, db *pgxpool.Pool) []Finding {
	var hasPriv bool
	if err := db.QueryRow(ctx, "SELECT has_schema_privilege('public','public','CREATE')").Scan(&hasPriv); err != nil {
		// Try alternate syntax
		if err2 := db.QueryRow(ctx, "SELECT has_schema_privilege('public','CREATE')").Scan(&hasPriv); err2 != nil {
			return []Finding{NewSkip("G11-003", g11, "Public schema CREATE privilege", err2.Error())}
		}
	}
	if hasPriv {
		return []Finding{NewWarn("G11-003", g11, "Public schema CREATE privilege",
			"PUBLIC role has CREATE privilege on the public schema",
			"REVOKE CREATE ON SCHEMA public FROM PUBLIC;",
			"Any authenticated user can create objects in the public schema, enabling privilege escalation.",
			"https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-PUBLIC")}
	}
	return []Finding{NewOK("G11-003", g11, "Public schema CREATE privilege",
		"PUBLIC does not have CREATE on the public schema",
		"https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-PUBLIC")}
}

// G11-004 rolcreaterole OR rolcreatedb AND NOT rolsuper
func g11PrivilegedRoles(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT rolname,
		CASE WHEN rolcreaterole THEN 'CREATEROLE' ELSE '' END ||
		CASE WHEN rolcreatedb THEN ' CREATEDB' ELSE '' END AS privs
		FROM pg_roles
		WHERE (rolcreaterole OR rolcreatedb) AND NOT rolsuper AND rolcanlogin
		ORDER BY 1`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G11-004", g11, "Privileged non-superuser roles", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var name, privs string
		_ = rows.Scan(&name, &privs)
		lines = append(lines, fmt.Sprintf("%s: %s", name, strings.TrimSpace(privs)))
	}
	if len(lines) > 0 {
		return []Finding{NewInfo("G11-004", g11, "Privileged non-superuser roles",
			fmt.Sprintf("%d role(s) with CREATEROLE or CREATEDB", len(lines)),
			"Review whether these roles need these privileges; prefer granting specific role memberships.",
			strings.Join(lines, "\n"),
			"https://www.postgresql.org/docs/current/sql-createrole.html")}
	}
	return []Finding{NewOK("G11-004", g11, "Privileged non-superuser roles",
		"No non-superuser roles with CREATEROLE/CREATEDB",
		"https://www.postgresql.org/docs/current/sql-createrole.html")}
}

// G11-005 log_connections=off AND log_disconnections=off
func g11ConnectionLogging(ctx context.Context, db *pgxpool.Pool) []Finding {
	var logConn, logDisconn string
	_ = db.QueryRow(ctx, "SELECT setting FROM pg_settings WHERE name='log_connections'").Scan(&logConn)
	_ = db.QueryRow(ctx, "SELECT setting FROM pg_settings WHERE name='log_disconnections'").Scan(&logDisconn)
	if logConn == "off" && logDisconn == "off" {
		return []Finding{NewWarn("G11-005", g11, "Connection logging",
			"log_connections=off AND log_disconnections=off",
			"Enable log_connections=on and log_disconnections=on for audit trail.",
			"Without connection logging, unauthorized access attempts will not be recorded.",
			"https://www.postgresql.org/docs/current/runtime-config-logging.html")}
	}
	return []Finding{NewOK("G11-005", g11, "Connection logging",
		fmt.Sprintf("log_connections=%s, log_disconnections=%s", logConn, logDisconn),
		"https://www.postgresql.org/docs/current/runtime-config-logging.html")}
}

// G11-006 pgaudit absent
func g11PgAudit(ctx context.Context, db *pgxpool.Pool) []Finding {
	var exists bool
	if err := db.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='pgaudit')").Scan(&exists); err != nil {
		return []Finding{NewSkip("G11-006", g11, "pgaudit extension", err.Error())}
	}
	if !exists {
		return []Finding{NewInfo("G11-006", g11, "pgaudit extension",
			"pgaudit is not installed",
			"Install pgaudit for comprehensive DDL/DML audit logging.",
			"pgaudit provides session-level and object-level audit logging for compliance requirements.",
			"https://github.com/pgaudit/pgaudit")}
	}
	return []Finding{NewOK("G11-006", g11, "pgaudit extension",
		"pgaudit is installed",
		"https://github.com/pgaudit/pgaudit")}
}

// G11-007 stale login accounts (advisory — requires pgaudit or external tooling)
func g11StaleLoginAccounts(ctx context.Context, db *pgxpool.Pool) []Finding {
	// Without pgaudit or last-login tracking, we can only report the total count of login roles
	const q = `SELECT count(*) FROM pg_roles WHERE rolcanlogin = true AND NOT rolsuper`
	var cnt int
	if err := db.QueryRow(ctx, q).Scan(&cnt); err != nil {
		return []Finding{NewSkip("G11-007", g11, "Stale login accounts", err.Error())}
	}
	return []Finding{NewInfo("G11-007", g11, "Stale login accounts",
		fmt.Sprintf("%d non-superuser login role(s) exist", cnt),
		"Periodically audit login roles; enable pgaudit to track last-login times.",
		"PostgreSQL does not natively track last login time; use pgaudit or application logging.",
		"https://www.postgresql.org/docs/current/database-roles.html")}
}

// G11-008 ssl_cert_file and ssl_key_file configured path info
func g11SSLCertPaths(ctx context.Context, db *pgxpool.Pool) []Finding {
	var certFile, keyFile string
	_ = db.QueryRow(ctx, "SELECT setting FROM pg_settings WHERE name='ssl_cert_file'").Scan(&certFile)
	_ = db.QueryRow(ctx, "SELECT setting FROM pg_settings WHERE name='ssl_key_file'").Scan(&keyFile)
	obs := fmt.Sprintf("ssl_cert_file=%q, ssl_key_file=%q", certFile, keyFile)
	if certFile == "" && keyFile == "" {
		return []Finding{NewInfo("G11-008", g11, "SSL certificate paths",
			"ssl_cert_file and ssl_key_file are not configured",
			"Configure ssl_cert_file and ssl_key_file in postgresql.conf to enable TLS.",
			"",
			"https://www.postgresql.org/docs/current/ssl-tcp.html")}
	}
	return []Finding{NewInfo("G11-008", g11, "SSL certificate paths",
		obs,
		"Verify certificate and key file permissions (key must be owner-readable only).",
		"The private key file should have mode 0600 and be owned by the postgres user.",
		"https://www.postgresql.org/docs/current/ssl-tcp.html")}
}
