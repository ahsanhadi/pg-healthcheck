// pg_healthcheck — enterprise PostgreSQL health diagnostics
//
// Usage examples:
//
//	pg_healthcheck --host db1 --dbname mydb --user postgres
//	pg_healthcheck --mode cluster --nodes node1:5432,node2:5432,node3:5432
//	pg_healthcheck --output json | jq '.summary'
//	pg_healthcheck --groups G01,G05,G09 --verbose
//
// Exit codes:
//
//	0  all OK
//	1  at least one WARN
//	2  at least one CRITICAL
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/pg_healthcheck/internal/checks"
	"github.com/pgedge/pg_healthcheck/internal/config"
	"github.com/pgedge/pg_healthcheck/internal/connector"
	"github.com/pgedge/pg_healthcheck/internal/report"
	"github.com/spf13/cobra"
)

// version information is injected at build time by GoReleaser via -ldflags.
// When building locally without GoReleaser these remain at their default values.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

// ── CLI flags ────────────────────────────────────────────────────────────────

var (
	flagHost          string
	flagPort          int
	flagDBName        string
	flagUser          string
	flagPassword      string
	flagMode          string
	flagNodes         string
	flagConfigFile    string
	flagOutput        string
	flagGroups        string
	flagTargetVersion int
	flagBackrestConf  string
	flagNoColor       bool
	flagVerbose       bool
)

// allCheckers lists every single-node check group in run order.
// To add a new group: implement checks.Checker and append it here.
var allCheckers = []checks.Checker{
	&checks.G01Connection{},
	&checks.G02Backrest{},
	&checks.G03Performance{},
	&checks.G04Locks{},
	&checks.G05Vacuum{},
	&checks.G06Indexes{},
	&checks.G07Toast{},
	&checks.G08Visibility{},
	&checks.G09WALSlots{},
	&checks.G10Upgrade{},
	&checks.G11Security{},
	&checks.G13OSResources{},
	&checks.G14WALGrowth{}, // WAL growth & generation rate
}

var spockChecker = &checks.G12SpockCluster{}

// ── Entry point ──────────────────────────────────────────────────────────────

func main() {
	root := &cobra.Command{
		Use:     "pg_healthcheck",
		Short:   "Enterprise PostgreSQL health diagnostics",
		Version: version + " (commit=" + commit + " built=" + date + ")",
		Long: `pg_healthcheck runs 90+ checks across 13 groups against a single PostgreSQL
instance or a pgEdge multi-node distributed cluster.

Every check queries real system catalog views — no estimated or simulated data.
Output is either coloured terminal text (default) or JSON for GUI / API use.`,
		SilenceUsage: true,
		RunE:         run,
	}

	f := root.Flags()
	f.StringVar(&flagHost, "host", "localhost", "PostgreSQL host")
	f.IntVar(&flagPort, "port", 5432, "PostgreSQL port")
	f.StringVar(&flagDBName, "dbname", "postgres", "Database name")
	f.StringVar(&flagUser, "user", "postgres", "Role name (or PGUSER env var)")
	f.StringVar(&flagPassword, "password", "", "Password (prefer PGPASSWORD env var)")
	f.StringVar(&flagMode, "mode", "single", "Run mode: single | cluster")
	f.StringVar(&flagNodes, "nodes", "", "Comma-separated host:port list for cluster mode")
	f.StringVar(&flagConfigFile, "config", "", "Path to YAML config file")
	f.StringVar(&flagOutput, "output", "text", "Output format: text | json")
	f.StringVar(&flagGroups, "groups", "", "Groups to run, e.g. G01,G05 (default: all)")
	f.IntVar(&flagTargetVersion, "target-version", 0, "Target PG major version for G10 upgrade checks")
	f.StringVar(&flagBackrestConf, "backrest-config", "", "Path to pgbackrest.conf")
	f.BoolVar(&flagNoColor, "no-color", false, "Disable terminal colour")
	f.BoolVar(&flagVerbose, "verbose", false, "Show OK findings (hidden by default)")

	if err := root.Execute(); err != nil {
		os.Exit(2)
	}
}

// ── run ──────────────────────────────────────────────────────────────────────

func run(cmd *cobra.Command, _ []string) error {
	// Build config: defaults → YAML file → CLI overrides
	cfg := buildConfig(cmd)

	// Validate group IDs before doing anything else.
	// This catches mistakes like `--groups --help` where pflag
	// consumes the next token as the flag value.
	if err := validateGroups(cfg.Groups, cmd); err != nil {
		return err
	}

	checkers := selectCheckers(cfg.Groups)

	var (
		allFindings []checks.Finding
		pgVersion   string
		hostname    string
	)

	switch cfg.Mode {
	case "cluster":
		allFindings, pgVersion, hostname = runCluster(cfg, checkers)
	default:
		allFindings, pgVersion, hostname = runSingle(cfg, checkers)
	}

	switch cfg.Output {
	case "json":
		if err := report.PrintJSON(allFindings, pgVersion, hostname, cfg.Mode, os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "json error: %v\n", err)
		}
	default:
		report.PrintText(allFindings, pgVersion, hostname, cfg.Mode, cfg.Verbose, cfg.NoColor)
	}

	os.Exit(report.ExitCode(allFindings))
	return nil
}

// ── Single-node run ──────────────────────────────────────────────────────────

func runSingle(cfg *config.Config, checkers []checks.Checker) ([]checks.Finding, string, string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, err := connector.Connect(ctx, cfg)
	if err != nil {
		f := checks.NewCrit("G01-001", "Connection & Availability",
			"TCP port reachability",
			fmt.Sprintf("Cannot connect to %s:%d: %v", cfg.Host, cfg.Port, err),
			"Verify PostgreSQL is running and credentials are correct.",
			"", "https://www.postgresql.org/docs/current/server-start.html")
		return []checks.Finding{f}, "unknown", fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	}
	defer db.Close()

	pgVer := pgVersion(ctx, db)
	host := pgHostname(ctx, db, cfg)

	var findings []checks.Finding
	for _, c := range checkers {
		cCtx, cCancel := context.WithTimeout(ctx, cfg.CheckTimeout)
		fs, err := c.Run(cCtx, db, cfg)
		cCancel()
		if err != nil {
			fs = []checks.Finding{checks.NewSkip(
				c.GroupID()+"-ERR", c.Name(), c.Name(), err.Error())}
		}
		findings = append(findings, fs...)
	}

	// G12 Spock checks — run cluster checker with a single-node slice.
	// Honour the --groups filter: skip G12 when a filter is active and G12 is not in it.
	if groupRequested(cfg.Groups, "G12") {
		cCtx, cCancel := context.WithTimeout(ctx, cfg.CheckTimeout)
		node := &checks.NodeConn{Name: host, Host: cfg.Host, Port: cfg.Port, DB: db}
		g12findings, _ := spockChecker.RunCluster(cCtx, []*checks.NodeConn{node}, cfg)
		findings = append(findings, g12findings...)
		cCancel()
	}

	return findings, pgVer, host
}

// ── Cluster run ──────────────────────────────────────────────────────────────

func runCluster(cfg *config.Config, checkers []checks.Checker) ([]checks.Finding, string, string) {
	if len(cfg.ClusterNodes) == 0 {
		fmt.Fprintln(os.Stderr, "error: cluster mode requires --nodes flag or cluster_nodes in config")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Connect to each node; failures emit CRITICAL but don't block other nodes
	var nodes []*checks.NodeConn
	var connFailFindings []checks.Finding

	for _, addr := range cfg.ClusterNodes {
		host, port := parseAddr(addr)
		db, err := connector.ConnectNode(ctx, host, port, cfg)
		if err != nil {
			connFailFindings = append(connFailFindings,
				checks.NewCrit("G01-001", "Connection & Availability",
					"TCP port reachability",
					fmt.Sprintf("[%s] Cannot connect: %v", addr, err),
					"Check network and PostgreSQL status on this node.",
					"", "https://www.postgresql.org/docs/current/server-start.html"))
			continue
		}
		nodes = append(nodes, &checks.NodeConn{
			Name: addr, Host: host, Port: port, DB: db,
		})
	}
	defer func() {
		for _, n := range nodes {
			n.DB.Close()
		}
	}()

	var allFindings []checks.Finding
	allFindings = append(allFindings, connFailFindings...)

	var pgVer, primaryHost string

	// Per-node single checks on each reachable node
	for _, node := range nodes {
		if pgVer == "" {
			pgVer = pgVersion(ctx, node.DB)
			primaryHost = node.Name
		}
		for _, c := range checkers {
			cCtx, cCancel := context.WithTimeout(ctx, cfg.CheckTimeout)
			fs, err := c.Run(cCtx, node.DB, cfg)
			cCancel()
			if err != nil {
				fs = []checks.Finding{checks.NewSkip(
					c.GroupID()+"-ERR", c.Name(), c.Name(), err.Error())}
			}
			for i := range fs {
				if fs[i].NodeName == "" {
					fs[i].NodeName = node.Name
				}
			}
			allFindings = append(allFindings, fs...)
		}
	}

	// G12 cross-node Spock checks — honour the --groups filter.
	if len(nodes) > 0 && groupRequested(cfg.Groups, "G12") {
		cCtx, cCancel := context.WithTimeout(ctx, 2*cfg.CheckTimeout)
		g12, _ := spockChecker.RunCluster(cCtx, nodes, cfg)
		cCancel()
		allFindings = append(allFindings, g12...)
	}

	return allFindings, pgVer, primaryHost
}

// ── Helpers ──────────────────────────────────────────────────────────────────

// buildConfig assembles the final Config from defaults, YAML file, and CLI flags.
func buildConfig(cmd *cobra.Command) *config.Config {
	cfg := config.Defaults()

	// Apply CLI connection defaults (will be overridden by YAML then re-overridden below)
	cfg.Host = flagHost
	cfg.Port = flagPort
	cfg.DBName = flagDBName
	cfg.User = flagUser
	cfg.Password = flagPassword
	cfg.Mode = flagMode
	cfg.Output = flagOutput
	cfg.NoColor = flagNoColor
	cfg.Verbose = flagVerbose
	cfg.TargetVersion = flagTargetVersion

	if flagBackrestConf != "" {
		cfg.BackrestConfig = flagBackrestConf
	}
	if flagNodes != "" {
		cfg.ClusterNodes = strings.Split(strings.TrimSpace(flagNodes), ",")
	}
	if flagGroups != "" {
		cfg.Groups = strings.Split(strings.TrimSpace(flagGroups), ",")
	}

	// Load YAML — falls back to ./healthcheck.yaml if no --config flag
	cfgPath := flagConfigFile
	if cfgPath == "" {
		if _, err := os.Stat("healthcheck.yaml"); err == nil {
			cfgPath = "healthcheck.yaml"
		}
	}
	if cfgPath != "" {
		if err := config.Load(cfgPath, cfg); err != nil {
			fmt.Fprintf(os.Stderr, "config warning: %v\n", err)
		}
	}

	// Re-apply CLI flags that the user explicitly set (they override YAML)
	if cmd.Flags().Changed("host") {
		cfg.Host = flagHost
	}
	if cmd.Flags().Changed("port") {
		cfg.Port = flagPort
	}
	if cmd.Flags().Changed("dbname") {
		cfg.DBName = flagDBName
	}
	if cmd.Flags().Changed("user") {
		cfg.User = flagUser
	}
	if cmd.Flags().Changed("password") {
		cfg.Password = flagPassword
	}
	if cmd.Flags().Changed("mode") {
		cfg.Mode = flagMode
	}
	if cmd.Flags().Changed("output") {
		cfg.Output = flagOutput
	}
	if cmd.Flags().Changed("nodes") {
		cfg.ClusterNodes = strings.Split(strings.TrimSpace(flagNodes), ",")
	}

	return cfg
}

// validateGroups checks that every requested group ID actually exists.
// This prevents silent "run everything" behaviour when the user passes
// something invalid (e.g. `--groups --help` where pflag grabs "--help"
// as the flag value rather than showing help).
func validateGroups(groups []string, cmd *cobra.Command) error {
	if len(groups) == 0 {
		return nil // no filter = run all
	}
	valid := make(map[string]bool)
	for _, c := range allCheckers {
		valid[c.GroupID()] = true
	}
	valid["G12"] = true // cluster checker not in allCheckers slice

	var unknown []string
	for _, g := range groups {
		id := strings.ToUpper(strings.TrimSpace(g))
		if !valid[id] {
			unknown = append(unknown, g)
		}
	}
	if len(unknown) > 0 {
		var ids []string
		for _, c := range allCheckers {
			ids = append(ids, c.GroupID())
		}
		ids = append(ids, "G12")
		return fmt.Errorf(
			"unknown group ID(s): %s\nValid groups: %s\nRun '%s --help' to see all flags.",
			strings.Join(unknown, ", "),
			strings.Join(ids, ", "),
			cmd.CommandPath(),
		)
	}
	return nil
}

// groupRequested reports whether id should run given the active groups filter.
// An empty/nil filter means "run all", so it always returns true.
func groupRequested(groups []string, id string) bool {
	if len(groups) == 0 {
		return true
	}
	for _, g := range groups {
		if strings.EqualFold(strings.TrimSpace(g), id) {
			return true
		}
	}
	return false
}

// selectCheckers filters allCheckers to only those in the groups slice.
// An empty/nil slice means "run all".
func selectCheckers(groups []string) []checks.Checker {
	if len(groups) == 0 {
		return allCheckers
	}
	want := make(map[string]bool)
	for _, g := range groups {
		want[strings.ToUpper(strings.TrimSpace(g))] = true
	}
	var out []checks.Checker
	for _, c := range allCheckers {
		if want[c.GroupID()] {
			out = append(out, c)
		}
	}
	return out
}

// pgVersion queries the server version string.
func pgVersion(ctx context.Context, db *pgxpool.Pool) string {
	var v string
	if err := db.QueryRow(ctx, "SELECT current_setting('server_version')").Scan(&v); err != nil {
		return "unknown"
	}
	return v
}

// pgHostname returns "host:port" using the pg_settings data_directory as a tiebreaker.
func pgHostname(_ context.Context, _ *pgxpool.Pool, cfg *config.Config) string {
	return fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
}

// parseAddr splits "host:port" into its components, defaulting to port 5432.
func parseAddr(addr string) (string, int) {
	addr = strings.TrimSpace(addr)
	idx := strings.LastIndex(addr, ":")
	if idx < 0 {
		return addr, 5432
	}
	port, err := strconv.Atoi(addr[idx+1:])
	if err != nil {
		return addr, 5432
	}
	return addr[:idx], port
}
