// Package config loads a node's settings from command-line flags and an
// optional YAML file. Precedence: defaults < config file < flags.
package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// ErrInvalid is wrapped by every validation error.
var ErrInvalid = errors.New("invalid config")

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

// Config holds everything a node needs to start.
type Config struct {
	// Listen is the address the gRPC server binds (clients AND peers use it).
	Listen string
	// Advertise is the address other nodes and clients use to reach this
	// node. It doubles as the node's Raft ID. Defaults to Listen unless that
	// binds a wildcard address (0.0.0.0), where it must be set explicitly.
	Advertise string
	// Peers are the Advertise addresses of the OTHER nodes. Empty means a
	// single-node cluster.
	Peers []string

	DataDir  string
	HTTPAddr string // health/metrics endpoint; empty disables it

	LogLevel  string // debug, info, warn, error
	LogFormat string // text or json

	// SnapshotThreshold is the number of applied entries between snapshots.
	// 0 selects the built-in default.
	SnapshotThreshold uint64

	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	ShutdownGrace      time.Duration

	// ShowVersion is set by -version; it is not valid in a config file.
	ShowVersion bool
}

// Defaults returns the default configuration: a single local node.
func Defaults() Config {
	return Config{
		Listen:             "127.0.0.1:7000",
		DataDir:            "data",
		LogLevel:           "info",
		LogFormat:          "text",
		ElectionTimeoutMin: 300 * time.Millisecond,
		ElectionTimeoutMax: 600 * time.Millisecond,
		HeartbeatInterval:  100 * time.Millisecond,
		ShutdownGrace:      10 * time.Second,
	}
}

type listValue struct{ p *[]string }

func (l listValue) String() string {
	if l.p == nil {
		return ""
	}
	return strings.Join(*l.p, ",")
}

func (l listValue) Set(s string) error {
	*l.p = splitList(s)
	return nil
}

func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// newFlagSet binds every setting to c. Each flag's default is c's CURRENT
// value, so defining the flags never overwrites values loaded from a file.
func newFlagSet(c *Config, path *string) *flag.FlagSet {
	fs := flag.NewFlagSet("raftkv-server", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	fs.StringVar(path, "config", *path, "path to a YAML config file (flags override it)")
	fs.StringVar(&c.Listen, "listen", c.Listen, "address to bind for clients and peers")
	fs.StringVar(&c.Advertise, "advertise", c.Advertise, "address peers and clients use to reach this node (default: -listen); also the node's ID")
	fs.Var(listValue{&c.Peers}, "peers", "comma-separated advertise addresses of the other nodes (empty = single-node cluster)")
	fs.StringVar(&c.DataDir, "data-dir", c.DataDir, "directory for the Raft log and snapshots")
	fs.StringVar(&c.HTTPAddr, "http-addr", c.HTTPAddr, "address for /healthz, /readyz, /status and /metrics (empty = disabled)")
	fs.StringVar(&c.LogLevel, "log-level", c.LogLevel, "debug, info, warn or error")
	fs.StringVar(&c.LogFormat, "log-format", c.LogFormat, "text or json")
	fs.Uint64Var(&c.SnapshotThreshold, "snapshot-threshold", c.SnapshotThreshold, "applied entries between snapshots (0 = default)")
	fs.DurationVar(&c.ElectionTimeoutMin, "election-timeout-min", c.ElectionTimeoutMin, "minimum election timeout")
	fs.DurationVar(&c.ElectionTimeoutMax, "election-timeout-max", c.ElectionTimeoutMax, "maximum election timeout")
	fs.DurationVar(&c.HeartbeatInterval, "heartbeat-interval", c.HeartbeatInterval, "leader heartbeat interval")
	fs.DurationVar(&c.ShutdownGrace, "shutdown-grace", c.ShutdownGrace, "max time to wait for in-flight requests on shutdown")
	fs.BoolVar(&c.ShowVersion, "version", false, "print version and exit")
	return fs
}

// Parse builds a Config from command-line arguments (without the program
// name). Usage text for -h is written to out; other errors are only returned.
// The result is validated, except when -version was requested.
func Parse(args []string, out io.Writer) (Config, error) {
	cfg := Defaults()
	var path string

	fs := newFlagSet(&cfg, &path)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(out, "Usage: raftkv-server [flags]\n\n")
			fs.SetOutput(out)
			fs.PrintDefaults()
		}
		return Config{}, err
	}

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read config file: %w", err)
		}
		cfg = Defaults()
		if err := ParseFile(data, &cfg); err != nil {
			return Config{}, fmt.Errorf("config file %s: %w", path, err)
		}
		// Parse the flags again on top of the file's values, so that any
		// flag given on the command line wins.
		fs = newFlagSet(&cfg, &path)
		if err := fs.Parse(args); err != nil {
			return Config{}, err
		}
	}

	if cfg.ShowVersion {
		return cfg, nil
	}
	if err := cfg.finalize(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// finalize fills in derived defaults, then validates.
func (c *Config) finalize() error {
	if c.Advertise == "" {
		host, _, err := net.SplitHostPort(c.Listen)
		if err != nil {
			return invalidf("listen %q: %v", c.Listen, err)
		}
		if isWildcard(host) {
			return invalidf("advertise is required when listen binds a wildcard address (%s)", c.Listen)
		}
		c.Advertise = c.Listen
	}
	return c.Validate()
}

// Validate checks every setting. Advertise must already be set.
func (c *Config) Validate() error {
	if err := checkAddr("listen", c.Listen, 0, true); err != nil {
		return err
	}
	if err := checkAddr("advertise", c.Advertise, 1, false); err != nil {
		return err
	}
	if c.HTTPAddr != "" {
		if err := checkAddr("http-addr", c.HTTPAddr, 0, true); err != nil {
			return err
		}
	}

	seen := map[string]bool{c.Advertise: true}
	for _, p := range c.Peers {
		if err := checkAddr("peer", p, 1, false); err != nil {
			return err
		}
		if seen[p] {
			return invalidf("peer %q is a duplicate or this node's own advertise address", p)
		}
		seen[p] = true
	}

	switch {
	case c.DataDir == "":
		return invalidf("data-dir must not be empty")
	case c.LogLevel != "debug" && c.LogLevel != "info" && c.LogLevel != "warn" && c.LogLevel != "error":
		return invalidf("log-level %q: want debug, info, warn or error", c.LogLevel)
	case c.LogFormat != "text" && c.LogFormat != "json":
		return invalidf("log-format %q: want text or json", c.LogFormat)
	case c.ElectionTimeoutMin <= 0 || c.ElectionTimeoutMax < c.ElectionTimeoutMin:
		return invalidf("election timeouts: need 0 < min <= max")
	case c.HeartbeatInterval <= 0 || c.HeartbeatInterval >= c.ElectionTimeoutMin:
		return invalidf("heartbeat-interval must be positive and below election-timeout-min")
	case c.ShutdownGrace <= 0:
		return invalidf("shutdown-grace must be positive")
	}
	return nil
}

func isWildcard(host string) bool {
	if host == "" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsUnspecified()
}

func checkAddr(field, addr string, minPort int, allowWildcard bool) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return invalidf("%s %q: %v", field, addr, err)
	}
	if p, err := strconv.Atoi(port); err != nil || p < minPort || p > 65535 {
		return invalidf("%s %q: bad port", field, addr)
	}
	if !allowWildcard && isWildcard(host) {
		return invalidf("%s %q: needs a concrete host name or IP", field, addr)
	}
	return nil
}

// Logger builds the slog logger described by the config, writing to w.
func (c Config) Logger(w io.Writer) *slog.Logger {
	level := slog.LevelInfo
	switch c.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	opts := &slog.HandlerOptions{Level: level}
	if c.LogFormat == "json" {
		return slog.New(slog.NewJSONHandler(w, opts))
	}
	return slog.New(slog.NewTextHandler(w, opts))
}
