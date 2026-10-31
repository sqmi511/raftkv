package config

import (
	"bytes"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestDefaultsAreValid(t *testing.T) {
	c := Defaults()
	if err := c.finalize(); err != nil {
		t.Fatalf("defaults are invalid: %v", err)
	}
	if c.Advertise != c.Listen {
		t.Fatalf("Advertise = %q, want it to default to Listen %q", c.Advertise, c.Listen)
	}
}

func TestValidation(t *testing.T) {
	cases := map[string]func(*Config){
		"wildcard listen without advertise": func(c *Config) { c.Listen = "0.0.0.0:7000" },
		"empty host listen without advert.": func(c *Config) { c.Listen = ":7000" },
		"listen without a port":             func(c *Config) { c.Listen = "localhost" },
		"listen with a bad port":            func(c *Config) { c.Listen = "127.0.0.1:99999" },
		"wildcard advertise":                func(c *Config) { c.Advertise = "0.0.0.0:7000" },
		"advertise port 0":                  func(c *Config) { c.Advertise = "127.0.0.1:0" },
		"peer is this node":                 func(c *Config) { c.Peers = []string{"127.0.0.1:7000"} },
		"duplicate peer":                    func(c *Config) { c.Peers = []string{"a:1", "a:1"} },
		"malformed peer":                    func(c *Config) { c.Peers = []string{"nope"} },
		"empty peer":                        func(c *Config) { c.Peers = []string{""} },
		"bad http addr":                     func(c *Config) { c.HTTPAddr = "nope" },
		"empty data dir":                    func(c *Config) { c.DataDir = "" },
		"bad log level":                     func(c *Config) { c.LogLevel = "loud" },
		"bad log format":                    func(c *Config) { c.LogFormat = "xml" },
		"max below min":                     func(c *Config) { c.ElectionTimeoutMax = c.ElectionTimeoutMin - 1 },
		"zero election timeout":             func(c *Config) { c.ElectionTimeoutMin = 0 },
		"heartbeat not below election":      func(c *Config) { c.HeartbeatInterval = c.ElectionTimeoutMin },
		"zero shutdown grace":               func(c *Config) { c.ShutdownGrace = 0 },
	}
	for name, mutate := range cases {
		c := Defaults()
		mutate(&c)
		if err := c.finalize(); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
	}

	ok := Defaults()
	ok.Listen = "0.0.0.0:7000"
	ok.Advertise = "node1:7000"
	ok.Peers = []string{"node2:7000", "node3:7000"}
	ok.HTTPAddr = "0.0.0.0:8000"
	if err := ok.finalize(); err != nil {
		t.Fatalf("a valid cluster config was rejected: %v", err)
	}
}

const sampleYAML = `# node 1
listen: 0.0.0.0:7000      # all interfaces
advertise: "node1:7000"
peers:
  - node2:7000
  - 'node3:7000'   # trailing comment
data-dir: /data
http-addr: 0.0.0.0:8000
log-level: debug
log-format: json
snapshot-threshold: 500
election-timeout-min: 400ms
election-timeout-max: 800ms
heartbeat-interval: 100ms
shutdown-grace: 3s
`

func TestParseFile(t *testing.T) {
	c := Defaults()
	if err := ParseFile([]byte(sampleYAML), &c); err != nil {
		t.Fatal(err)
	}
	want := Defaults()
	want.Listen = "0.0.0.0:7000"
	want.Advertise = "node1:7000"
	want.Peers = []string{"node2:7000", "node3:7000"}
	want.DataDir = "/data"
	want.HTTPAddr = "0.0.0.0:8000"
	want.LogLevel = "debug"
	want.LogFormat = "json"
	want.SnapshotThreshold = 500
	want.ElectionTimeoutMin = 400 * time.Millisecond
	want.ElectionTimeoutMax = 800 * time.Millisecond
	want.HeartbeatInterval = 100 * time.Millisecond
	want.ShutdownGrace = 3 * time.Second
	if !reflect.DeepEqual(c, want) {
		t.Fatalf("parsed config mismatch:\n got %+v\nwant %+v", c, want)
	}
	if err := c.finalize(); err != nil {
		t.Fatalf("the sample config is invalid: %v", err)
	}
}

func TestParseFileFlowListAndEmptyValues(t *testing.T) {
	c := Defaults()
	err := ParseFile([]byte("peers: [a:1, \"b:2\"]   # two peers\nhttp-addr: \"\"\n"), &c)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c.Peers, []string{"a:1", "b:2"}) || c.HTTPAddr != "" {
		t.Fatalf("got peers %v, http-addr %q", c.Peers, c.HTTPAddr)
	}

	c = Defaults()
	c.Peers = []string{"x:1"}
	if err := ParseFile([]byte("peers:\n"), &c); err != nil || len(c.Peers) != 0 {
		t.Fatalf("an empty peers block should clear the list: %v, %v", c.Peers, err)
	}

	c = Defaults()
	if err := ParseFile([]byte("peers: a:1, b:2\n"), &c); err != nil || len(c.Peers) != 2 {
		t.Fatalf("a comma-separated scalar should work: %v, %v", c.Peers, err)
	}
}

func TestParseFileErrors(t *testing.T) {
	cases := []struct {
		name, doc, want string
	}{
		{"unknown key", "listen: a:1\nbogus: 1\n", "line 2: unknown key"},
		{"duplicate key", "listen: a:1\nlisten: b:2\n", "line 2: duplicate key"},
		{"missing colon", "listen\n", "line 1"},
		{"indented key", "  listen: a:1\n", "indentation"},
		{"orphan list item", "- x\n", "list item without a key"},
		{"empty list item", "peers:\n  -\n", "empty list item"},
		{"bad duration", "heartbeat-interval: soon\n", "line 1"},
		{"bad number", "snapshot-threshold: many\n", "snapshot-threshold"},
		{"unterminated flow list", "peers: [a, b\n", "unterminated"},
		{"unterminated quote", "listen: \"abc\n", "unterminated quote"},
		{"text after quote", "listen: \"a\" b\n", "after the closing quote"},
		{"list for a scalar", "listen:\n  - a:1\n", "single value"},
		{"missing scalar value", "data-dir:\n", "needs a value"},
	}
	for _, tc := range cases {
		c := Defaults()
		err := ParseFile([]byte(tc.doc), &c)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want it to contain %q", tc.name, err, tc.want)
		}
	}
}

func TestParseFlags(t *testing.T) {
	c, err := Parse([]string{
		"-listen", "127.0.0.1:7001",
		"-peers", "127.0.0.1:7002, 127.0.0.1:7003",
		"-http-addr", "127.0.0.1:8001",
		"-data-dir", "d1",
		"-snapshot-threshold", "250",
		"-heartbeat-interval", "20ms",
		"-election-timeout-min", "100ms",
		"-election-timeout-max", "200ms",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if c.Advertise != "127.0.0.1:7001" || !reflect.DeepEqual(c.Peers, []string{"127.0.0.1:7002", "127.0.0.1:7003"}) ||
		c.SnapshotThreshold != 250 || c.HeartbeatInterval != 20*time.Millisecond {
		t.Fatalf("parsed config = %+v", c)
	}
}

func TestFlagsOverrideConfigFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.yaml")
	doc := "listen: 127.0.0.1:7100\npeers:\n  - 127.0.0.1:7101\n  - 127.0.0.1:7102\ndata-dir: from-file\nlog-level: warn\n"
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}

	c, err := Parse([]string{"-config", path, "-listen", "127.0.0.1:7200", "-log-level", "debug"}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if c.Listen != "127.0.0.1:7200" || c.LogLevel != "debug" {
		t.Fatalf("flags did not override the file: listen %q, level %q", c.Listen, c.LogLevel)
	}
	if c.DataDir != "from-file" || len(c.Peers) != 2 {
		t.Fatalf("file values were lost: data-dir %q, peers %v", c.DataDir, c.Peers)
	}
	if c.Advertise != "127.0.0.1:7200" {
		t.Fatalf("Advertise = %q, want it derived from the final listen address", c.Advertise)
	}
}

func TestParseErrors(t *testing.T) {
	var out bytes.Buffer
	if _, err := Parse([]string{"-h"}, &out); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("-h err = %v, want flag.ErrHelp", err)
	}
	for _, want := range []string{"Usage:", "-listen", "-peers", "-config"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("usage text is missing %q:\n%s", want, out.String())
		}
	}

	if _, err := Parse([]string{"-nope"}, &bytes.Buffer{}); err == nil {
		t.Fatal("an unknown flag was accepted")
	}
	if _, err := Parse([]string{"-config", filepath.Join(t.TempDir(), "missing.yaml")}, &bytes.Buffer{}); err == nil {
		t.Fatal("a missing config file was accepted")
	}
	if _, err := Parse([]string{"-peers", "127.0.0.1:7000"}, &bytes.Buffer{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("listing ourselves as a peer: err = %v, want ErrInvalid", err)
	}

	bad := filepath.Join(t.TempDir(), "bad.yaml")
	if err := os.WriteFile(bad, []byte("typo-key: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Parse([]string{"-config", bad}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), bad) {
		t.Fatalf("err = %v, want it to name the config file", err)
	}
}

func TestVersionSkipsValidation(t *testing.T) {
	c, err := Parse([]string{"-version", "-listen", "not-an-address"}, &bytes.Buffer{})
	if err != nil || !c.ShowVersion {
		t.Fatalf("Parse(-version) = %+v, %v", c, err)
	}
}

func TestLogger(t *testing.T) {
	var buf bytes.Buffer
	c := Defaults()
	c.LogFormat = "json"
	c.LogLevel = "warn"
	log := c.Logger(&buf)
	log.Info("hidden")
	log.Warn("shown", "k", "v")

	out := buf.String()
	if strings.Contains(out, "hidden") || !strings.Contains(out, `"msg":"shown"`) || !strings.Contains(out, `"k":"v"`) {
		t.Fatalf("log output = %q", out)
	}

	buf.Reset()
	c.LogFormat, c.LogLevel = "text", "debug"
	c.Logger(&buf).Debug("dbg")
	if !strings.Contains(buf.String(), "msg=dbg") {
		t.Fatalf("text log output = %q", buf.String())
	}
}
