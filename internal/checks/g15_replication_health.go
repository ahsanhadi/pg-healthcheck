package checks

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/pg_healthcheck/internal/config"
)

const g15 = "Replication Health"

// G15ReplicationHealth checks streaming replication connectivity and standby conflict health.
type G15ReplicationHealth struct{}

func (g *G15ReplicationHealth) Name() string    { return g15 }
func (g *G15ReplicationHealth) GroupID() string { return "G15" }

func (g *G15ReplicationHealth) Run(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) ([]Finding, error) {
	var f []Finding
	f = append(f, g15WALReceiverStatus(ctx, db)...)
	f = append(f, g15InactivePhysicalSlots(ctx, db)...)
	f = append(f, g15ConflictsByType(ctx, db)...)
	return f, nil
}

// G15-001 WAL receiver status on standby
// If pg_is_in_recovery()=true but pg_stat_wal_receiver has no rows, the standby
// is not receiving WAL — it is silently falling behind the primary.
func g15WALReceiverStatus(ctx context.Context, db *pgxpool.Pool) []Finding {
	var inRecovery bool
	if err := db.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery); err != nil {
		return []Finding{NewSkip("G15-001", g15, "WAL receiver status", err.Error())}
	}
	if !inRecovery {
		return []Finding{NewOK("G15-001", g15, "WAL receiver status",
			"Not a standby — check runs on standby nodes only",
			"https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-WAL-RECEIVER-VIEW")}
	}

	var count int
	if err := db.QueryRow(ctx, "SELECT count(*) FROM pg_stat_wal_receiver").Scan(&count); err != nil {
		return []Finding{NewSkip("G15-001", g15, "WAL receiver status", err.Error())}
	}
	if count == 0 {
		return []Finding{NewCrit("G15-001", g15, "WAL receiver status",
			"Standby has no active WAL receiver process",
			"Check PostgreSQL logs on the standby; verify the primary is reachable and recovery.conf/standby.signal is correct.",
			"A standby without a WAL receiver is not replicating and falls further behind until reconnected.",
			"https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-WAL-RECEIVER-VIEW")}
	}

	const q = `SELECT status, sender_host,
		COALESCE(EXTRACT(EPOCH FROM (now() - last_msg_receipt_time))::bigint, -1) AS last_msg_age_secs
		FROM pg_stat_wal_receiver LIMIT 1`
	var status, senderHost string
	var lastMsgAgeSecs int64
	if err := db.QueryRow(ctx, q).Scan(&status, &senderHost, &lastMsgAgeSecs); err != nil {
		return []Finding{NewSkip("G15-001", g15, "WAL receiver status", err.Error())}
	}
	obs := fmt.Sprintf("status=%s sender=%s last_msg=%s ago", status, senderHost, g15FmtSecs(lastMsgAgeSecs))
	if lastMsgAgeSecs > 300 {
		return []Finding{NewWarn("G15-001", g15, "WAL receiver status", obs,
			"WAL receiver has not heard from the primary in >5 minutes; check network and primary logs.",
			fmt.Sprintf("Last message from primary was %s ago", g15FmtSecs(lastMsgAgeSecs)),
			"https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-WAL-RECEIVER-VIEW")}
	}
	return []Finding{NewOK("G15-001", g15, "WAL receiver status", obs,
		"https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-WAL-RECEIVER-VIEW")}
}

// G15-002 Inactive physical replication slots on primary
// Physical slots represent streaming standbys. An inactive physical slot means the standby
// has disconnected and WAL will accumulate indefinitely. G09-001 does not catch physical
// slots because confirmed_flush_lsn is NULL for them.
func g15InactivePhysicalSlots(ctx context.Context, db *pgxpool.Pool) []Finding {
	var inRecovery bool
	_ = db.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery)
	if inRecovery {
		return []Finding{NewOK("G15-002", g15, "Inactive physical replication slots",
			"Node is a standby — check runs on primary only",
			"https://www.postgresql.org/docs/current/view-pg-replication-slots.html")}
	}

	const q = `SELECT slot_name,
		pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) / 1024 / 1024 AS lag_mb
		FROM pg_replication_slots
		WHERE slot_type = 'physical' AND active = false
		ORDER BY lag_mb DESC`
	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G15-002", g15, "Inactive physical replication slots", err.Error())}
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var slotName string
		var lagMB int64
		_ = rows.Scan(&slotName, &lagMB)
		lines = append(lines, fmt.Sprintf("%s: lag=%dMB", slotName, lagMB))
	}
	if err := rows.Err(); err != nil {
		return []Finding{NewSkip("G15-002", g15, "Inactive physical replication slots", "scan error: "+err.Error())}
	}
	if len(lines) > 0 {
		return []Finding{NewCrit("G15-002", g15, "Inactive physical replication slots",
			fmt.Sprintf("%d inactive physical slot(s) — standby(s) are disconnected", len(lines)),
			"Reconnect the standby or drop the unused slot to prevent WAL accumulation and disk exhaustion.",
			strings.Join(lines, "\n"),
			"https://www.postgresql.org/docs/current/view-pg-replication-slots.html")}
	}
	return []Finding{NewOK("G15-002", g15, "Inactive physical replication slots",
		"All physical replication slots are active",
		"https://www.postgresql.org/docs/current/view-pg-replication-slots.html")}
}

// G15-003 pg_stat_database_conflicts breakdown by type (standby only)
// On standbys, queries are cancelled to allow vacuum/cleanup to proceed on the primary.
// Breaking down conflicts by type gives actionable root-cause information:
//   - confl_snapshot: primary vacuum is reclaiming rows still visible to standby queries → enable hot_standby_feedback
//   - confl_bufferpin: standby queries hold buffer pins too long → reduce long queries on standby
//   - confl_active_logicalslot (PG16+): a logical decoding slot blocks standby cleanup → drop unused slots
func g15ConflictsByType(ctx context.Context, db *pgxpool.Pool) []Finding {
	var inRecovery bool
	_ = db.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery)
	if !inRecovery {
		return []Finding{NewOK("G15-003", g15, "Standby query conflicts by type",
			"Not a standby — pg_stat_database_conflicts is only populated on standbys",
			"https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-DATABASE-CONFLICTS-VIEW")}
	}

	var major int
	_ = db.QueryRow(ctx, "SELECT current_setting('server_version_num')::int/10000").Scan(&major)

	var q string
	if major >= 16 {
		q = `SELECT datname,
			coalesce(confl_tablespace, 0),
			coalesce(confl_lock, 0),
			coalesce(confl_snapshot, 0),
			coalesce(confl_bufferpin, 0),
			coalesce(confl_deadlock, 0),
			coalesce(confl_active_logicalslot, 0)
			FROM pg_stat_database_conflicts
			WHERE datname NOT IN ('template0', 'template1')
			ORDER BY (confl_lock + confl_snapshot + confl_bufferpin + confl_deadlock + confl_active_logicalslot) DESC`
	} else {
		q = `SELECT datname,
			coalesce(confl_tablespace, 0),
			coalesce(confl_lock, 0),
			coalesce(confl_snapshot, 0),
			coalesce(confl_bufferpin, 0),
			coalesce(confl_deadlock, 0),
			0 AS confl_active_logicalslot
			FROM pg_stat_database_conflicts
			WHERE datname NOT IN ('template0', 'template1')
			ORDER BY (confl_lock + confl_snapshot + confl_bufferpin + confl_deadlock) DESC`
	}

	rows, err := db.Query(ctx, q)
	if err != nil {
		return []Finding{NewSkip("G15-003", g15, "Standby query conflicts by type", err.Error())}
	}
	defer rows.Close()

	var warnLines []string
	var totalSnap, totalLock, totalPin, totalDead, totalLogical int64
	for rows.Next() {
		var datname string
		var conflTablespace, conflLock, conflSnapshot, conflBufferpin, conflDeadlock, conflLogical int64
		if err := rows.Scan(&datname, &conflTablespace, &conflLock, &conflSnapshot,
			&conflBufferpin, &conflDeadlock, &conflLogical); err != nil {
			continue
		}
		rowTotal := conflLock + conflSnapshot + conflBufferpin + conflDeadlock + conflLogical
		if rowTotal > 0 {
			warnLines = append(warnLines, fmt.Sprintf(
				"%s: snapshot=%d lock=%d bufferpin=%d deadlock=%d logicalslot=%d",
				datname, conflSnapshot, conflLock, conflBufferpin, conflDeadlock, conflLogical))
		}
		totalSnap += conflSnapshot
		totalLock += conflLock
		totalPin += conflBufferpin
		totalDead += conflDeadlock
		totalLogical += conflLogical
	}
	if err := rows.Err(); err != nil {
		return []Finding{NewSkip("G15-003", g15, "Standby query conflicts by type", "scan error: "+err.Error())}
	}

	grandTotal := totalSnap + totalLock + totalPin + totalDead + totalLogical
	obs := fmt.Sprintf("total=%d (snapshot=%d lock=%d bufferpin=%d deadlock=%d logicalslot=%d)",
		grandTotal, totalSnap, totalLock, totalPin, totalDead, totalLogical)

	if len(warnLines) == 0 {
		return []Finding{NewOK("G15-003", g15, "Standby query conflicts by type",
			"No query conflicts recorded on this standby",
			"https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-DATABASE-CONFLICTS-VIEW")}
	}

	var detail string
	switch {
	case totalLogical > 0:
		detail = "confl_active_logicalslot is non-zero: a logical decoding slot is blocking standby cleanup. Drop unused logical slots on the primary."
	case totalSnap >= totalPin && totalSnap >= totalLock:
		detail = "confl_snapshot dominates: primary vacuum is reclaiming rows still visible to standby queries. Enable hot_standby_feedback=on or increase max_standby_streaming_delay."
	case totalPin > totalSnap && totalPin >= totalLock:
		detail = "confl_bufferpin dominates: long-running standby queries hold buffer pins. Reduce query duration on this standby."
	default:
		detail = "Mixed conflict types — review hot_standby_feedback and max_standby_streaming_delay."
	}

	return []Finding{NewWarn("G15-003", g15, "Standby query conflicts by type",
		obs,
		"Increase max_standby_streaming_delay or enable hot_standby_feedback=on to reduce conflict cancellations.",
		detail+"\n"+strings.Join(warnLines, "\n"),
		"https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-DATABASE-CONFLICTS-VIEW")}
}

func g15FmtSecs(secs int64) string {
	if secs < 0 {
		return "unknown"
	}
	if secs < 60 {
		return fmt.Sprintf("%ds", secs)
	}
	if secs < 3600 {
		return fmt.Sprintf("%dm%ds", secs/60, secs%60)
	}
	return fmt.Sprintf("%dh%dm", secs/3600, (secs%3600)/60)
}
