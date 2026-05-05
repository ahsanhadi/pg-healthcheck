package checks

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/pg-healthcheck/internal/config"
	"gopkg.in/ini.v1"
)

const g02 = "pgBackRest Configuration & WAL Archiving"

// G02Backrest checks pgBackRest configuration and WAL archiving health.
type G02Backrest struct{}

func (g *G02Backrest) Name() string    { return g02 }
func (g *G02Backrest) GroupID() string { return "G02" }

func (g *G02Backrest) Run(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) ([]Finding, error) {
	iniCfg, err := ini.Load(cfg.BackrestConfig)
	if err != nil {
		return []Finding{NewInfo("G02-000", g02, "pgBackRest not detected",
			fmt.Sprintf("pgBackRest config not found at %s — all G02 checks are skipped.", cfg.BackrestConfig),
			"This is expected on environments not using pgBackRest for backup management. "+
				"If pgBackRest is installed, set the correct config path via backrest_config in healthcheck.yaml.",
			"All 14 G02 checks cover pgBackRest-specific settings (archive-async, spool-path, retention, "+
				"WAL archiving health etc.) and are not applicable without pgBackRest.",
			"https://pgbackrest.org/configuration.html")}, nil
	}

	var f []Finding
	f = append(f, g02ArchiveAsync(iniCfg, cfg)...)
	f = append(f, g02SpoolPath(iniCfg, cfg)...)
	f = append(f, g02ProcessMax(iniCfg)...)
	f = append(f, g02CompressType(iniCfg, cfg)...)
	f = append(f, g02CompressLevel(iniCfg, cfg)...)
	f = append(f, g02BufferSize(iniCfg, cfg)...)
	f = append(f, g02BackupStandby(iniCfg, cfg)...)
	f = append(f, g02ArchiveCommand(ctx, db)...)
	f = append(f, g02WALReadyBacklog(ctx, db, cfg)...)
	f = append(f, g02OldestReadyAge(ctx, db)...)
	f = append(f, g02ArchiverFailures(ctx, db)...)
	f = append(f, g02LastBackupAge(cfg)...)
	f = append(f, g02RetentionFull(iniCfg, cfg)...)
	f = append(f, g02ArchivePushQueueMax(iniCfg, cfg)...)
	return f, nil
}

// getIniVal checks the stanza section first, then [global].
func getIniVal(cfg *ini.File, stanza, key string) string {
	if sec, err := cfg.GetSection(stanza); err == nil {
		if k, err := sec.GetKey(key); err == nil {
			return strings.TrimSpace(k.Value())
		}
	}
	if sec, err := cfg.GetSection("global"); err == nil {
		if k, err := sec.GetKey(key); err == nil {
			return strings.TrimSpace(k.Value())
		}
	}
	return ""
}

// G02-001 archive-async
func g02ArchiveAsync(iniCfg *ini.File, cfg *config.Config) []Finding {
	val := getIniVal(iniCfg, cfg.BackrestStanza, "archive-async")
	if val != "y" {
		return []Finding{NewWarn("G02-001", g02, "archive-async",
			fmt.Sprintf("archive-async = %q", val),
			"Set archive-async=y in pgbackrest.conf to prevent WAL archiving from blocking transactions.",
			"Synchronous archiving adds latency to every WAL segment write.",
			"https://pgbackrest.org/configuration.html#section-archive/option-archive-async")}
	}
	return []Finding{NewOK("G02-001", g02, "archive-async", "archive-async = y",
		"https://pgbackrest.org/configuration.html#section-archive/option-archive-async")}
}

// G02-002 spool-path
func g02SpoolPath(iniCfg *ini.File, cfg *config.Config) []Finding {
	async := getIniVal(iniCfg, cfg.BackrestStanza, "archive-async")
	if async != "y" {
		return []Finding{NewSkip("G02-002", g02, "spool-path", "archive-async is not enabled")}
	}
	spool := getIniVal(iniCfg, cfg.BackrestStanza, "spool-path")
	if spool == "" {
		return []Finding{NewCrit("G02-002", g02, "spool-path",
			"spool-path not set but archive-async=y",
			"Set spool-path to a fast local directory (e.g. /var/spool/pgbackrest).",
			"Without spool-path, async archiving cannot buffer WAL segments.",
			"https://pgbackrest.org/configuration.html#section-general/option-spool-path")}
	}
	if _, err := os.Stat(spool); err != nil {
		return []Finding{NewCrit("G02-002", g02, "spool-path",
			fmt.Sprintf("spool-path = %s (does not exist: %v)", spool, err),
			"Create the spool directory and ensure pgBackRest can write to it.",
			"",
			"https://pgbackrest.org/configuration.html#section-general/option-spool-path")}
	}
	return []Finding{NewOK("G02-002", g02, "spool-path",
		fmt.Sprintf("spool-path = %s", spool),
		"https://pgbackrest.org/configuration.html#section-general/option-spool-path")}
}

// G02-003 process-max vs runtime.NumCPU()
func g02ProcessMax(iniCfg *ini.File) []Finding {
	val := getIniVal(iniCfg, "global", "process-max")
	cur, _ := strconv.Atoi(val)
	nCPU := runtime.NumCPU()
	minRec := 2
	if nCPU/4 > minRec {
		minRec = nCPU / 4
	}
	obs := fmt.Sprintf("process-max = %d (CPUs: %d, recommended >= %d)", cur, nCPU, minRec)
	if cur < minRec {
		return []Finding{NewWarn("G02-003", g02, "process-max",
			obs,
			fmt.Sprintf("Set process-max >= %d (max(2, nCPU/4)) for better backup/restore throughput.", minRec),
			"",
			"https://pgbackrest.org/configuration.html#section-general/option-process-max")}
	}
	return []Finding{NewOK("G02-003", g02, "process-max", obs,
		"https://pgbackrest.org/configuration.html#section-general/option-process-max")}
}

// G02-004 compress-type
func g02CompressType(iniCfg *ini.File, cfg *config.Config) []Finding {
	val := getIniVal(iniCfg, cfg.BackrestStanza, "compress-type")
	if val == "" {
		val = "gz"
	}
	if val == "gz" {
		return []Finding{NewInfo("G02-004", g02, "compress-type",
			"compress-type = gz",
			"Consider switching to compress-type=zst for better compression ratio and speed.",
			"zstd typically achieves 30-50% better compression than gzip with lower CPU overhead.",
			"https://pgbackrest.org/configuration.html#section-compress/option-compress-type")}
	}
	return []Finding{NewOK("G02-004", g02, "compress-type",
		fmt.Sprintf("compress-type = %s", val),
		"https://pgbackrest.org/configuration.html#section-compress/option-compress-type")}
}

// G02-005 compress-level
func g02CompressLevel(iniCfg *ini.File, cfg *config.Config) []Finding {
	val := getIniVal(iniCfg, cfg.BackrestStanza, "compress-level")
	level, _ := strconv.Atoi(val)
	if val != "" && level >= 6 {
		return []Finding{NewInfo("G02-005", g02, "compress-level",
			fmt.Sprintf("compress-level = %d", level),
			"Consider compress-level=3 for zst or level=1-3 for faster backups with acceptable ratio.",
			"High compression levels significantly increase CPU usage during backup.",
			"https://pgbackrest.org/configuration.html#section-compress/option-compress-level")}
	}
	obs := fmt.Sprintf("compress-level = %s", val)
	if val == "" {
		obs = "compress-level = (default)"
	}
	return []Finding{NewOK("G02-005", g02, "compress-level", obs,
		"https://pgbackrest.org/configuration.html#section-compress/option-compress-level")}
}

// G02-006 buffer-size
func g02BufferSize(iniCfg *ini.File, cfg *config.Config) []Finding {
	val := getIniVal(iniCfg, cfg.BackrestStanza, "buffer-size")
	size, _ := strconv.ParseInt(val, 10, 64)
	const minSize = 4194304 // 4 MiB
	if val != "" && size < minSize {
		return []Finding{NewWarn("G02-006", g02, "buffer-size",
			fmt.Sprintf("buffer-size = %s (%d bytes)", val, size),
			fmt.Sprintf("Set buffer-size >= %d (4 MiB) for efficient I/O.", minSize),
			"Small buffer sizes reduce backup throughput.",
			"https://pgbackrest.org/configuration.html#section-general/option-buffer-size")}
	}
	obs := fmt.Sprintf("buffer-size = %s", val)
	if val == "" {
		obs = "buffer-size = (default)"
	}
	return []Finding{NewOK("G02-006", g02, "buffer-size", obs,
		"https://pgbackrest.org/configuration.html#section-general/option-buffer-size")}
}

// G02-007 backup-standby
func g02BackupStandby(iniCfg *ini.File, cfg *config.Config) []Finding {
	if len(cfg.ClusterNodes) <= 1 {
		return []Finding{NewSkip("G02-007", g02, "backup-standby", "Single-node cluster; backup-standby not applicable")}
	}
	val := getIniVal(iniCfg, cfg.BackrestStanza, "backup-standby")
	if val != "y" {
		return []Finding{NewInfo("G02-007", g02, "backup-standby",
			fmt.Sprintf("backup-standby = %q (cluster has %d nodes)", val, len(cfg.ClusterNodes)),
			"Set backup-standby=y to reduce I/O load on the primary during backups.",
			"With multiple nodes, backups from a standby keep the primary free for writes.",
			"https://pgbackrest.org/configuration.html#section-backup/option-backup-standby")}
	}
	return []Finding{NewOK("G02-007", g02, "backup-standby",
		fmt.Sprintf("backup-standby = y (cluster has %d nodes)", len(cfg.ClusterNodes)),
		"https://pgbackrest.org/configuration.html#section-backup/option-backup-standby")}
}

// G02-008 archive_command SQL check
func g02ArchiveCommand(ctx context.Context, db *pgxpool.Pool) []Finding {
	var cmd string
	if err := db.QueryRow(ctx, "SELECT setting FROM pg_settings WHERE name='archive_command'").Scan(&cmd); err != nil {
		return []Finding{NewSkip("G02-008", g02, "archive_command", err.Error())}
	}
	cmd = strings.TrimSpace(cmd)
	bad := cmd == "" || cmd == "true" || cmd == "/bin/true" || cmd == "(disabled)"
	if bad {
		return []Finding{NewCrit("G02-008", g02, "archive_command",
			fmt.Sprintf("archive_command = %q", cmd),
			"Set archive_command to a valid pgbackrest archive-push command.",
			"WAL is not being archived; point-in-time recovery is impossible.",
			"https://www.postgresql.org/docs/current/continuous-archiving.html")}
	}
	return []Finding{NewOK("G02-008", g02, "archive_command",
		fmt.Sprintf("archive_command = %s", cmd),
		"https://www.postgresql.org/docs/current/continuous-archiving.html")}
}

// G02-009 WAL .ready file backlog
func g02WALReadyBacklog(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) []Finding {
	var dataDir string
	if err := db.QueryRow(ctx, "SELECT setting FROM pg_settings WHERE name='data_directory'").Scan(&dataDir); err != nil {
		return []Finding{NewSkip("G02-009", g02, "WAL .ready file backlog", err.Error())}
	}
	statusDir := filepath.Join(dataDir, "pg_wal", "archive_status")
	entries, err := os.ReadDir(statusDir)
	if err != nil {
		return []Finding{NewSkip("G02-009", g02, "WAL .ready file backlog",
			fmt.Sprintf("Cannot read %s: %v", statusDir, err))}
	}
	var count int
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".ready") {
			count++
		}
	}
	obs := fmt.Sprintf("%d .ready files in %s", count, statusDir)
	if count >= cfg.WALReadyCritCount {
		return []Finding{NewCrit("G02-009", g02, "WAL .ready file backlog", obs,
			"Check pgbackrest archive-push; archiving is severely lagging.",
			"WAL segment accumulation will eventually fill disk.",
			"https://pgbackrest.org/user-guide.html#archiving")}
	}
	if count >= cfg.WALReadyWarnCount {
		return []Finding{NewWarn("G02-009", g02, "WAL .ready file backlog", obs,
			fmt.Sprintf("Investigate archiving lag; threshold is %d.", cfg.WALReadyWarnCount),
			"",
			"https://pgbackrest.org/user-guide.html#archiving")}
	}
	return []Finding{NewOK("G02-009", g02, "WAL .ready file backlog", obs,
		"https://pgbackrest.org/user-guide.html#archiving")}
}

// G02-010 oldest .ready file age
func g02OldestReadyAge(ctx context.Context, db *pgxpool.Pool) []Finding {
	var dataDir string
	if err := db.QueryRow(ctx, "SELECT setting FROM pg_settings WHERE name='data_directory'").Scan(&dataDir); err != nil {
		return []Finding{NewSkip("G02-010", g02, "Oldest .ready file age", err.Error())}
	}
	statusDir := filepath.Join(dataDir, "pg_wal", "archive_status")
	entries, err := os.ReadDir(statusDir)
	if err != nil {
		return []Finding{NewSkip("G02-010", g02, "Oldest .ready file age",
			fmt.Sprintf("Cannot read %s: %v", statusDir, err))}
	}
	var oldest time.Time
	var oldestName string
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".ready") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if oldest.IsZero() || info.ModTime().Before(oldest) {
			oldest = info.ModTime()
			oldestName = e.Name()
		}
	}
	if oldest.IsZero() {
		return []Finding{NewOK("G02-010", g02, "Oldest .ready file age",
			"No .ready files found",
			"https://pgbackrest.org/user-guide.html#archiving")}
	}
	age := time.Since(oldest)
	obs := fmt.Sprintf("Oldest: %s (age: %s)", oldestName, age.Round(time.Second))
	if age > time.Hour {
		return []Finding{NewWarn("G02-010", g02, "Oldest .ready file age", obs,
			"Oldest unarchived WAL segment is over 1 hour old; check archiving process.",
			"",
			"https://pgbackrest.org/user-guide.html#archiving")}
	}
	return []Finding{NewOK("G02-010", g02, "Oldest .ready file age", obs,
		"https://pgbackrest.org/user-guide.html#archiving")}
}

// G02-011 pg_stat_archiver failures
func g02ArchiverFailures(ctx context.Context, db *pgxpool.Pool) []Finding {
	const q = `SELECT failed_count, last_failed_wal,
		EXTRACT(EPOCH FROM (now() - last_failed_time))::int
		FROM pg_stat_archiver`
	var failCount int
	var lastFailedWal *string
	var lastFailedAge *int
	if err := db.QueryRow(ctx, q).Scan(&failCount, &lastFailedWal, &lastFailedAge); err != nil {
		return []Finding{NewSkip("G02-011", g02, "pg_stat_archiver failures", err.Error())}
	}
	if failCount > 0 {
		detail := ""
		if lastFailedWal != nil {
			detail = fmt.Sprintf("Last failed WAL: %s", *lastFailedWal)
		}
		if lastFailedAge != nil {
			detail += fmt.Sprintf(" (%ds ago)", *lastFailedAge)
		}
		return []Finding{NewWarn("G02-011", g02, "pg_stat_archiver failures",
			fmt.Sprintf("%d archive failure(s) recorded", failCount),
			"Check PostgreSQL logs for archive_command errors.",
			detail,
			"https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ARCHIVER-VIEW")}
	}
	return []Finding{NewOK("G02-011", g02, "pg_stat_archiver failures",
		"No archiver failures recorded",
		"https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ARCHIVER-VIEW")}
}

// backrestInfo represents the JSON output from `pgbackrest info`.
type backrestInfo struct {
	Backup []struct {
		Timestamp struct {
			Stop int64 `json:"stop"`
		} `json:"timestamp"`
		Type string `json:"type"`
	} `json:"backup"`
	Name string `json:"name"`
}

// G02-012 last backup age via `pgbackrest info --output=json`
func g02LastBackupAge(cfg *config.Config) []Finding {
	args := []string{"info", "--output=json", "--stanza=" + cfg.BackrestStanza}
	if cfg.BackrestConfig != "" {
		args = append(args, "--config="+cfg.BackrestConfig)
	}
	out, err := exec.Command("pgbackrest", args...).Output() //#nosec G204
	if err != nil {
		return []Finding{NewWarn("G02-012", g02, "Last backup age",
			fmt.Sprintf("pgbackrest info failed: %v", err),
			"Ensure pgbackrest is installed and the stanza is configured.",
			"",
			"https://pgbackrest.org/command.html#command-info")}
	}
	var infos []backrestInfo
	if err := json.Unmarshal(out, &infos); err != nil {
		return []Finding{NewWarn("G02-012", g02, "Last backup age",
			fmt.Sprintf("Cannot parse pgbackrest info JSON: %v", err),
			"", "",
			"https://pgbackrest.org/command.html#command-info")}
	}
	var lastStop int64
	for _, info := range infos {
		if info.Name != cfg.BackrestStanza {
			continue
		}
		for _, b := range info.Backup {
			if b.Timestamp.Stop > lastStop {
				lastStop = b.Timestamp.Stop
			}
		}
	}
	if lastStop == 0 {
		return []Finding{NewCrit("G02-012", g02, "Last backup age",
			"No backups found for stanza "+cfg.BackrestStanza,
			"Run an immediate full backup: pgbackrest --stanza="+cfg.BackrestStanza+" backup --type=full",
			"",
			"https://pgbackrest.org/command.html#command-backup")}
	}
	lastBackup := time.Unix(lastStop, 0)
	age := time.Since(lastBackup)
	obs := fmt.Sprintf("Last backup: %s (age: %s)", lastBackup.Format(time.RFC3339), age.Round(time.Minute))
	maxAge := time.Duration(cfg.BackupMaxAgeHours) * time.Hour
	if age > maxAge {
		return []Finding{NewCrit("G02-012", g02, "Last backup age", obs,
			fmt.Sprintf("Run a backup immediately; last backup is older than %dh.", cfg.BackupMaxAgeHours),
			"",
			"https://pgbackrest.org/command.html#command-backup")}
	}
	return []Finding{NewOK("G02-012", g02, "Last backup age", obs,
		"https://pgbackrest.org/command.html#command-backup")}
}

// G02-013 repo-retention-full
func g02RetentionFull(iniCfg *ini.File, cfg *config.Config) []Finding {
	val := getIniVal(iniCfg, cfg.BackrestStanza, "repo1-retention-full")
	if val == "" {
		val = getIniVal(iniCfg, cfg.BackrestStanza, "repo-retention-full")
	}
	ret, _ := strconv.Atoi(val)
	obs := fmt.Sprintf("repo-retention-full = %s", val)
	if val == "" {
		obs = "repo-retention-full = (not set)"
	}
	if ret < cfg.MinRetentionFull {
		return []Finding{NewWarn("G02-013", g02, "repo-retention-full", obs,
			fmt.Sprintf("Set repo-retention-full >= %d for adequate recovery points.", cfg.MinRetentionFull),
			"",
			"https://pgbackrest.org/configuration.html#section-expire/option-repo-retention-full")}
	}
	return []Finding{NewOK("G02-013", g02, "repo-retention-full", obs,
		"https://pgbackrest.org/configuration.html#section-expire/option-repo-retention-full")}
}

// G02-014 archive-push-queue-max info
func g02ArchivePushQueueMax(iniCfg *ini.File, cfg *config.Config) []Finding {
	val := getIniVal(iniCfg, cfg.BackrestStanza, "archive-push-queue-max")
	if val == "" {
		return []Finding{NewInfo("G02-014", g02, "archive-push-queue-max",
			"archive-push-queue-max = (not set — unlimited)",
			"Set archive-push-queue-max to cap disk usage for WAL queue (e.g. 4GB).",
			"Without a limit, a stuck archiver can fill the disk with WAL segments.",
			"https://pgbackrest.org/configuration.html#section-archive/option-archive-push-queue-max")}
	}
	return []Finding{NewOK("G02-014", g02, "archive-push-queue-max",
		fmt.Sprintf("archive-push-queue-max = %s", val),
		"https://pgbackrest.org/configuration.html#section-archive/option-archive-push-queue-max")}
}
