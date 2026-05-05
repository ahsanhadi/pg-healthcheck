// Package checks defines the shared types (Finding, Checker) and helper
// constructors used by every check group.
package checks

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/pg-healthcheck/internal/config"
	"github.com/pgedge/pg-healthcheck/internal/severity"
)

// ── Finding ──────────────────────────────────────────────────────────────────

// Finding is one health-check result emitted by a check group.
// The JSON tags drive the --output json format consumed by dashboards/GUIs.
type Finding struct {
	CheckID     string            `json:"check_id"`
	Group       string            `json:"group"`
	Severity    severity.Severity `json:"severity"`
	Title       string            `json:"title"`
	Observed    string            `json:"observed"`
	Recommended string            `json:"recommended,omitempty"`
	Detail      string            `json:"detail,omitempty"`
	DocURL      string            `json:"doc_url,omitempty"`
	NodeName    string            `json:"node_name,omitempty"` // cluster mode only
}

// ── Constructors ─────────────────────────────────────────────────────────────

func NewOK(id, group, title, observed, docURL string) Finding {
	return Finding{CheckID: id, Group: group, Severity: severity.OK,
		Title: title, Observed: observed, DocURL: docURL}
}
func NewInfo(id, group, title, observed, recommended, detail, docURL string) Finding {
	return Finding{CheckID: id, Group: group, Severity: severity.INFO,
		Title: title, Observed: observed, Recommended: recommended,
		Detail: detail, DocURL: docURL}
}
func NewWarn(id, group, title, observed, recommended, detail, docURL string) Finding {
	return Finding{CheckID: id, Group: group, Severity: severity.WARN,
		Title: title, Observed: observed, Recommended: recommended,
		Detail: detail, DocURL: docURL}
}
func NewCrit(id, group, title, observed, recommended, detail, docURL string) Finding {
	return Finding{CheckID: id, Group: group, Severity: severity.CRITICAL,
		Title: title, Observed: observed, Recommended: recommended,
		Detail: detail, DocURL: docURL}
}

// NewSkip creates an INFO finding for a check that could not run
// (extension missing, permission denied, timeout, etc.).
// Skipped checks are informational — they are not real warnings.
func NewSkip(id, group, title, reason string) Finding {
	return Finding{
		CheckID:     id,
		Group:       group,
		Severity:    severity.INFO,
		Title:       fmt.Sprintf("Skipped — %s", title),
		Observed:    reason,
		Recommended: "Resolve the reason above to enable this check.",
	}
}

// ── Interfaces ───────────────────────────────────────────────────────────────

// Checker is implemented by every single-node check group (G01–G11, G13).
type Checker interface {
	Name() string    // human label, e.g. "Connection & Availability"
	GroupID() string // short ID, e.g. "G01"
	Run(ctx context.Context, db *pgxpool.Pool, cfg *config.Config) ([]Finding, error)
}

// NodeConn pairs a human-readable name with a live connection pool.
// Used by the cluster runner and G12.
type NodeConn struct {
	Name string // e.g. "node1:5432"
	Host string
	Port int
	DB   *pgxpool.Pool
}

// ClusterChecker is implemented only by G12SpockCluster.
// It receives connections to ALL nodes so it can do cross-node diffs.
type ClusterChecker interface {
	Name() string
	GroupID() string
	RunCluster(ctx context.Context, nodes []*NodeConn, cfg *config.Config) ([]Finding, error)
}
