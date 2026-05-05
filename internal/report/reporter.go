// Package report renders health-check findings as coloured terminal output
// or as a single buffered JSON object (for GUI / API consumption).
package report

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/pgedge/pg-healthcheck/internal/checks"
	"github.com/pgedge/pg-healthcheck/internal/severity"
)

// ── JSON types ───────────────────────────────────────────────────────────────

// Report is the top-level JSON object emitted with --output json.
// Every field maps directly to something a GUI can render.
type Report struct {
	Timestamp string           `json:"timestamp"`
	Hostname  string           `json:"hostname"`
	PGVersion string           `json:"pg_version"`
	Mode      string           `json:"mode"` // "single" | "cluster"
	Checks    []checks.Finding `json:"checks"`
	Summary   Summary          `json:"summary"`
}

// Summary counts findings by severity level.
type Summary struct {
	OK       int `json:"ok"`
	Info     int `json:"info"`
	Warn     int `json:"warn"`
	Critical int `json:"critical"`
	Total    int `json:"total"`
}

// ── Terminal colours ─────────────────────────────────────────────────────────

var (
	cOK   = color.New(color.FgGreen, color.Bold)
	cInfo = color.New(color.FgCyan)
	cWarn = color.New(color.FgYellow, color.Bold)
	cCrit = color.New(color.FgRed, color.Bold)
	cHead = color.New(color.FgWhite, color.Bold)
	cDim  = color.New(color.Faint)
)

func sevColor(s severity.Severity) *color.Color {
	switch s {
	case severity.OK:
		return cOK
	case severity.INFO:
		return cInfo
	case severity.WARN:
		return cWarn
	case severity.CRITICAL:
		return cCrit
	default:
		return color.New(color.Reset)
	}
}

func sevIcon(s severity.Severity) string {
	switch s {
	case severity.OK:
		return "✓"
	case severity.INFO:
		return "ⓘ"
	case severity.WARN:
		return "⚠"
	case severity.CRITICAL:
		return "✗"
	default:
		return "?"
	}
}

// ── Text output ──────────────────────────────────────────────────────────────

const ruleLine = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

// PrintText writes coloured tabular output to stdout.
//   - Groups findings by check group, sorts by severity (critical first).
//   - Hides OK findings unless verbose=true.
//   - Prints a summary table at the end.
func PrintText(findings []checks.Finding, pgVersion, hostname, mode string, verbose, noColor bool) {
	if noColor {
		color.NoColor = true
	}

	// ── header ──────────────────────────────────────────────
	fmt.Println()
	cHead.Printf("  pg-healthcheck")
	fmt.Printf("  │  %s  │  PG %s  │  %s  │  %s\n",
		hostname, pgVersion, mode,
		time.Now().Format("2006-01-02 15:04:05 UTC"))
	fmt.Println(cDim.Sprint(ruleLine))

	// ── group findings ──────────────────────────────────────
	groups, order := groupAndSort(findings)
	counts := map[severity.Severity]int{}

	for _, grp := range order {
		gf := groups[grp]
		for _, f := range gf {
			counts[f.Severity]++
		}
		// Only print the group header when there is something non-OK to show
		// (or when verbose=true)
		visibles := filterVisible(gf, verbose)
		if len(visibles) == 0 {
			continue
		}
		fmt.Println()
		cHead.Printf("  %s\n", strings.ToUpper(grp))
		fmt.Println(cDim.Sprint("  " + strings.Repeat("─", 62)))
		for _, f := range visibles {
			printFinding(f, verbose)
		}
	}

	// ── summary ─────────────────────────────────────────────
	fmt.Println()
	fmt.Println(cDim.Sprint(ruleLine))
	cHead.Println("  SUMMARY")
	fmt.Println(cDim.Sprint("  " + strings.Repeat("─", 62)))
	fmt.Println()

	ok := counts[severity.OK]
	info := counts[severity.INFO]
	warn := counts[severity.WARN]
	crit := counts[severity.CRITICAL]
	total := ok + info + warn + crit

	fmt.Print("  ")
	cOK.Printf("✓ OK %d   ", ok)
	cInfo.Printf("ⓘ INFO %d   ", info)
	cWarn.Printf("⚠ WARN %d   ", warn)
	cCrit.Printf("✗ CRITICAL %d", crit)
	cDim.Printf("   (total: %d)\n\n", total)

	switch {
	case crit > 0:
		cCrit.Printf("  ✗  %d CRITICAL finding(s) require IMMEDIATE attention.\n\n", crit)
	case warn > 0:
		cWarn.Printf("  ⚠  %d WARN finding(s) should be fixed before the next incident window.\n\n", warn)
	default:
		cOK.Printf("  ✓  All checks passed.\n\n")
	}

	// Composite WAL pipeline alerts — emitted when two related groups are
	// simultaneously CRITICAL, indicating a combined failure scenario.
	printCompositeAlerts(findings)
}

// printCompositeAlerts scans findings for co-occurring CRITICALs across
// the three WAL-related groups (G02, G09, G14) and prints contextual banners.
func printCompositeAlerts(findings []checks.Finding) {
	critGroups := map[string]bool{}
	for _, f := range findings {
		if f.Severity == severity.CRITICAL {
			critGroups[f.Group] = true
		}
	}

	g02crit := critGroups["pgBackRest Backup"]
	g09crit := critGroups["WAL & Replication Slots"]
	g14crit := critGroups["WAL Growth & Generation Rate"]

	if !g02crit && !g09crit && !g14crit {
		return // nothing to do
	}

	fmt.Println(cDim.Sprint(ruleLine))
	cHead.Println("  COMPOSITE ALERTS")
	fmt.Println(cDim.Sprint("  " + strings.Repeat("─", 62)))
	fmt.Println()

	if g02crit && g14crit {
		cCrit.Println("  ✗  COMPOSITE CRITICAL: WAL pipeline under simultaneous pressure")
		fmt.Println("     G02 (archiving) + G14 (generation) are both CRITICAL.")
		cDim.Println("     → WAL is generating faster than it is being archived.")
		cDim.Println("     → pg_wal WILL fill. Immediate action required.")
		fmt.Println()
	}

	if g09crit && g14crit {
		cCrit.Println("  ✗  COMPOSITE CRITICAL: Inactive slot + disk filling")
		fmt.Println("     G09 (slot retention) + G14 (disk space) are both CRITICAL.")
		cDim.Println("     → An inactive replication slot is retaining WAL AND the disk is >80% full.")
		cDim.Println("     → Drop or advance the inactive slot immediately.")
		fmt.Println()
	}
}

// groupAndSort returns findings keyed by group, preserving declaration order.
func groupAndSort(findings []checks.Finding) (map[string][]checks.Finding, []string) {
	groups := make(map[string][]checks.Finding)
	var order []string
	seen := make(map[string]bool)
	for _, f := range findings {
		if !seen[f.Group] {
			seen[f.Group] = true
			order = append(order, f.Group)
		}
		groups[f.Group] = append(groups[f.Group], f)
	}
	// Within each group: critical first, then warn, info, ok; then by check ID
	for grp := range groups {
		sort.Slice(groups[grp], func(i, j int) bool {
			fi, fj := groups[grp][i], groups[grp][j]
			if fi.Severity != fj.Severity {
				return fi.Severity > fj.Severity
			}
			return fi.CheckID < fj.CheckID
		})
	}
	return groups, order
}

func filterVisible(findings []checks.Finding, verbose bool) []checks.Finding {
	if verbose {
		return findings
	}
	var out []checks.Finding
	for _, f := range findings {
		if f.Severity != severity.OK {
			out = append(out, f)
		}
	}
	return out
}

// truncate shortens s to max runes, appending "…" if trimmed.
func truncate(s string, max int) string {
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	return string(runes[:max-1]) + "…"
}

func printFinding(f checks.Finding, verbose bool) {
	col := sevColor(f.Severity)
	icon := sevIcon(f.Severity)

	node := ""
	if f.NodeName != "" {
		node = "[" + f.NodeName + "] "
	}

	// One line per finding: icon  CheckID  Title  Observed
	// Observed is truncated so long error strings don't wrap the terminal.
	obs := truncate(node+f.Observed, 65)
	col.Printf("  %s %-8s  %-38s  %s\n", icon, f.CheckID, truncate(f.Title, 38), obs)

	// In verbose mode show the recommendation, detail, and doc link.
	if verbose {
		if f.Recommended != "" {
			cDim.Printf("             → %s\n", f.Recommended)
		}
		if f.Detail != "" {
			for _, line := range strings.Split(strings.TrimSpace(f.Detail), "\n") {
				cDim.Printf("               %s\n", line)
			}
		}
		if f.DocURL != "" {
			cDim.Printf("               docs: %s\n", f.DocURL)
		}
	}
}

// ── JSON output ──────────────────────────────────────────────────────────────

// PrintJSON writes a single, fully-buffered JSON object to w.
// Partial JSON is never emitted — the full payload is marshalled in memory first.
func PrintJSON(findings []checks.Finding, pgVersion, hostname, mode string, w io.Writer) error {
	if w == nil {
		w = os.Stdout
	}
	s := Summary{}
	for _, f := range findings {
		s.Total++
		switch f.Severity {
		case severity.OK:
			s.OK++
		case severity.INFO:
			s.Info++
		case severity.WARN:
			s.Warn++
		case severity.CRITICAL:
			s.Critical++
		}
	}
	rpt := Report{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Hostname:  hostname,
		PGVersion: pgVersion,
		Mode:      mode,
		Checks:    findings,
		Summary:   s,
	}
	data, err := json.MarshalIndent(rpt, "", "  ")
	if err != nil {
		return fmt.Errorf("marshalling JSON report: %w", err)
	}
	_, err = fmt.Fprintln(w, string(data))
	return err
}

// ── Exit code ────────────────────────────────────────────────────────────────

// ExitCode returns 0 (all OK), 1 (any WARN), or 2 (any CRITICAL).
func ExitCode(findings []checks.Finding) int {
	code := 0
	for _, f := range findings {
		switch f.Severity {
		case severity.CRITICAL:
			return 2
		case severity.WARN:
			if code < 1 {
				code = 1
			}
		}
	}
	return code
}
