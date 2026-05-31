// Package nlp maps natural-language queries to pg-healthcheck group IDs.
// It tries an Ollama LLM first and falls back to keyword matching for
// air-gapped environments where Ollama is unavailable.
package nlp

import "strings"

// groupMeta holds a group's name and the keywords that trigger it.
type groupMeta struct {
	name     string
	keywords []string
}

// groupIndex is the authoritative list of groups and their trigger keywords.
// Keywords are lowercase; matching is case-insensitive and substring-based.
var groupIndex = map[string]groupMeta{
	"G01": {
		name: "Connection & Availability",
		keywords: []string{
			"connection", "connect", "ssl", "tls", "certificate", "cert",
			"port", "reachab", "idle in transaction", "idle transaction",
			"pg_hba", "trust auth", "isready", "pg_isready", "latency",
		},
	},
	"G02": {
		name: "Backups & Recovery",
		keywords: []string{
			"backup", "backrest", "pgbackrest", "archive", "wal archiv",
			"recovery", "restore", "pitr", "retention",
		},
	},
	"G03": {
		name: "Performance",
		keywords: []string{
			"performance", "autovacuum", "slow query", "query stat",
			"planner", "statistics", "pg_stat_statements",
		},
	},
	"G04": {
		name: "Locks",
		keywords: []string{
			"lock", "deadlock", "block", "contention", "wait",
			"lock wait", "blocked query",
		},
	},
	"G05": {
		name: "Vacuum & Bloat",
		keywords: []string{
			"vacuum", "dead tuple", "bloat", "txid", "wraparound",
			"prepared transaction", "table bloat", "autovacuum", "freeze",
		},
	},
	"G06": {
		name: "Indexes",
		keywords: []string{
			"index", "duplicate index", "invalid index", "primary key",
			"missing pk", "index bloat", "unused index", "btree",
		},
	},
	"G07": {
		name: "TOAST & Corruption",
		keywords: []string{
			"toast", "corruption", "corrupt", "checksum", "amcheck",
			"b-tree", "btree verify", "integrity", "data checksum",
			"checksum failure", "orphan",
		},
	},
	"G08": {
		name: "Visibility Map",
		keywords: []string{
			"visibility", "pg_visibility", "visibility map", "all-visible",
		},
	},
	"G09": {
		name: "WAL & Replication Slots",
		keywords: []string{
			"wal slot", "replication slot", "slot lag", "logical replication",
			"subscription", "inactive slot", "slot retain", "logical slot",
			"physical slot", "slot",
		},
	},
	"G10": {
		name: "Upgrade Readiness",
		keywords: []string{
			"upgrade", "pg_upgrade", "version upgrade", "upgrade readiness",
			"major version",
		},
	},
	"G11": {
		name: "Security",
		keywords: []string{
			"security", "superuser", "permission", "schema permission",
			"public schema", "trust auth", "role", "privilege", "grant",
			"pg_hba",
		},
	},
	"G12": {
		name: "Spock Cluster",
		keywords: []string{
			"spock", "pgedge", "pgEdge", "multi-node", "logical cluster",
			"spock cluster", "distributed",
		},
	},
	"G13": {
		name: "OS Resources",
		keywords: []string{
			"disk", "disk space", "memory", "os resource", "swap",
			"background writer", "shared_buffers", "shared buffers",
			"filesystem", "inode", "cpu",
		},
	},
	"G14": {
		name: "WAL Growth",
		keywords: []string{
			"wal growth", "wal size", "wal disk", "wal generat", "wal rate",
			"checkpoint", "wal filesystem", "fpi", "full page", "wal dir",
			"wal accumul", "pg_wal",
		},
	},
	"G15": {
		name: "Replication Health",
		keywords: []string{
			"replication health", "streaming replica", "replica", "standby",
			"recovery state", "replication lag", "wal receiver",
		},
	},
}

// KeywordMatch returns group IDs whose keywords appear in query.
// The query is lowercased before matching. Multiple groups may match.
func KeywordMatch(query string) []string {
	q := strings.ToLower(query)
	seen := make(map[string]bool)
	var result []string

	// Preserve G01–G15 order for deterministic output.
	ordered := []string{
		"G01", "G02", "G03", "G04", "G05", "G06", "G07",
		"G08", "G09", "G10", "G11", "G12", "G13", "G14", "G15",
	}
	for _, id := range ordered {
		meta := groupIndex[id]
		for _, kw := range meta.keywords {
			if strings.Contains(q, kw) && !seen[id] {
				seen[id] = true
				result = append(result, id)
				break
			}
		}
	}
	return result
}

// GroupName returns the human-readable name for a group ID, or the ID itself if unknown.
func GroupName(id string) string {
	if m, ok := groupIndex[strings.ToUpper(id)]; ok {
		return m.name
	}
	return id
}
