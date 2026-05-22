package nlp

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/pgedge/pg-healthcheck/internal/config"
)

// promptTemplate is sent to Ollama to map a user query to group IDs.
// It lists all groups with their scope so the model can make an informed choice.
const promptTemplate = `You are a PostgreSQL health-check assistant. Map the user query to one or more of the check groups listed below.

Available check groups:
G01 - Connection & Availability: TCP reachability, SSL/TLS certificates, connection saturation, idle-in-transaction sessions, pg_hba trust auth
G02 - Backups & Recovery: pgBackRest backup age, WAL archiving failures, recovery configuration
G03 - Performance: Autovacuum effectiveness, slow query statistics, query planner stats
G04 - Locks: Lock contention, deadlocks, blocking queries, long-running blocked sessions
G05 - Vacuum & Bloat: Dead tuple accumulation, table bloat, prepared transactions, TXID wraparound risk
G06 - Indexes: Duplicate indexes, invalid indexes, missing primary keys, index bloat
G07 - TOAST & Corruption: Data checksums, checksum failures, TOAST table reference integrity, orphaned TOAST tables, amcheck B-tree verification
G08 - Visibility Map: pg_visibility consistency checks
G09 - WAL & Replication Slots: Physical/logical replication slot lag, inactive slots, subscription health, streaming lag
G10 - Upgrade Readiness: pg_upgrade pre-check compatibility
G11 - Security: Superuser count, public schema CREATE permissions, TRUST authentication methods
G12 - Spock Cluster: pgEdge Spock multi-node logical replication cluster health
G13 - OS Resources: Disk space, shared memory, background writer, OS-level memory and swap
G14 - WAL Growth: WAL directory size, WAL generation rate, FPI ratio, checkpoint health, pg_wal filesystem usage
G15 - Replication Health: Streaming replica recovery state and replication lag

User query: %q

Reply with ONLY a comma-separated list of matching group IDs (e.g. G07,G14). No explanation, no markdown, just the IDs.`

// Source identifies how the group mapping was resolved.
type Source string

const (
	// SourceLLM means a cloud or local LLM provider returned the groups.
	SourceLLM Source = "llm"
	// SourceKeyword means the built-in keyword matcher was used (LLM unavailable or unset).
	SourceKeyword Source = "keyword"
)

// MapResult is the outcome of mapping a natural-language query to check groups.
type MapResult struct {
	Groups       []string
	Source       Source
	ProviderName string // e.g. "ollama/llama3.2", "openai/gpt-4o-mini"; empty for keyword
}

// MapQuery maps a natural-language query to check group IDs.
// It tries the configured LLM provider first (Ollama, OpenAI, or Gemini).
// If the provider is unavailable, misconfigured, or returns no valid group IDs,
// it falls back to built-in keyword matching automatically.
func MapQuery(query string, cfg *config.Config) (MapResult, error) {
	prompt := fmt.Sprintf(promptTemplate, query)

	provider, err := NewProvider(cfg)
	if err == nil {
		raw, qErr := provider.Ask(prompt)
		if qErr == nil {
			if groups := parseGroupIDs(raw); len(groups) > 0 {
				return MapResult{
					Groups:       groups,
					Source:       SourceLLM,
					ProviderName: provider.Name(),
				}, nil
			}
		}
	}

	// Fallback: keyword matching (always works, no network required).
	groups := KeywordMatch(query)
	if len(groups) == 0 {
		return MapResult{}, fmt.Errorf("no matching check groups found for: %q\nTry terms like 'toast', 'wal', 'replication', 'locks', 'vacuum'", query)
	}
	return MapResult{Groups: groups, Source: SourceKeyword}, nil
}

// groupIDRe matches G01–G15 anywhere in an LLM response, handling any delimiter
// or punctuation the model might use.
var groupIDRe = regexp.MustCompile(`(?i)\bG(0[1-9]|1[0-5])\b`)

// parseGroupIDs extracts valid group IDs (G01–G15) from an LLM response string.
// Uses a regex so it works regardless of delimiter (comma, space, semicolon, etc.).
func parseGroupIDs(response string) []string {
	seen := make(map[string]bool)
	var result []string
	for _, m := range groupIDRe.FindAllString(response, -1) {
		id := strings.ToUpper(m)
		if !seen[id] {
			seen[id] = true
			result = append(result, id)
		}
	}
	return result
}
