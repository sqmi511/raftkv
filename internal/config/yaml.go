package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// This file reads a small subset of YAML, enough for a flat config file:
//
//	# comment
//	listen: 0.0.0.0:7000        # trailing comment
//	advertise: "node1:7000"     # single or double quotes are allowed
//	peers:                      # lists: block style ...
//	  - node2:7000
//	  - node3:7000
//	# ... or flow style:  peers: [node2:7000, node3:7000]
//
// Nested maps, multi-line strings, anchors and the like are not supported.
// A value that starts with "[" is read as a flow list, so quote IPv6
// addresses: "[::1]:7000". Keys use the same names as the flags. Unknown or
// repeated keys are errors, which catches typos.

type yamlEntry struct {
	line   int
	key    string
	value  string
	list   []string
	isList bool
}

// ParseFile applies the settings in a YAML document to c.
func ParseFile(data []byte, c *Config) error {
	entries, err := parseYAML(data)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := applyEntry(c, e); err != nil {
			return fmt.Errorf("line %d: %w", e.line, err)
		}
	}
	return nil
}

func parseYAML(data []byte) ([]yamlEntry, error) {
	var entries []yamlEntry
	seen := map[string]int{}
	cur := -1 // index of the entry that "- item" lines belong to

	for i, raw := range strings.Split(string(data), "\n") {
		lineNo := i + 1
		line := strings.TrimRight(raw, " \t\r")
		trimmed := strings.TrimLeft(line, " \t")
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		if trimmed == "-" || strings.HasPrefix(trimmed, "- ") {
			if cur < 0 {
				return nil, fmt.Errorf("line %d: list item without a key", lineNo)
			}
			item, err := scalar(strings.TrimSpace(trimmed[1:]))
			if err != nil {
				return nil, fmt.Errorf("line %d: %w", lineNo, err)
			}
			if item == "" {
				return nil, fmt.Errorf("line %d: empty list item", lineNo)
			}
			entries[cur].list = append(entries[cur].list, item)
			continue
		}

		if len(trimmed) != len(line) {
			return nil, fmt.Errorf("line %d: unexpected indentation (nested values are not supported)", lineNo)
		}
		colon := strings.Index(line, ":")
		if colon <= 0 {
			return nil, fmt.Errorf("line %d: expected \"key: value\"", lineNo)
		}
		key := strings.TrimSpace(line[:colon])
		rest := strings.TrimSpace(line[colon+1:])
		if prev, dup := seen[key]; dup {
			return nil, fmt.Errorf("line %d: duplicate key %q (first on line %d)", lineNo, key, prev)
		}
		seen[key] = lineNo

		switch {
		case rest == "" || rest[0] == '#':
			// A block list follows (or the list is empty).
			entries = append(entries, yamlEntry{line: lineNo, key: key, isList: true})
			cur = len(entries) - 1

		case rest[0] == '[':
			body := stripComment(rest)
			if !strings.HasSuffix(body, "]") {
				return nil, fmt.Errorf("line %d: unterminated [ list", lineNo)
			}
			var items []string
			for _, part := range strings.Split(body[1:len(body)-1], ",") {
				if part = strings.TrimSpace(part); part == "" {
					continue
				}
				item, err := scalar(part)
				if err != nil {
					return nil, fmt.Errorf("line %d: %w", lineNo, err)
				}
				items = append(items, item)
			}
			entries = append(entries, yamlEntry{line: lineNo, key: key, isList: true, list: items})
			cur = -1

		default:
			v, err := scalar(rest)
			if err != nil {
				return nil, fmt.Errorf("line %d: %w", lineNo, err)
			}
			entries = append(entries, yamlEntry{line: lineNo, key: key, value: v})
			cur = -1
		}
	}
	return entries, nil
}

// stripComment removes a trailing " # comment".
func stripComment(s string) string {
	if i := strings.Index(s, " #"); i >= 0 {
		return strings.TrimSpace(s[:i])
	}
	return s
}

// scalar reads a plain or quoted value.
func scalar(s string) (string, error) {
	if s == "" {
		return "", nil
	}
	if q := s[0]; q == '"' || q == '\'' {
		end := strings.IndexByte(s[1:], q)
		if end < 0 {
			return "", fmt.Errorf("unterminated quote in %s", s)
		}
		if rest := strings.TrimSpace(s[end+2:]); rest != "" && rest[0] != '#' {
			return "", fmt.Errorf("unexpected text after the closing quote in %s", s)
		}
		return s[1 : 1+end], nil
	}
	return stripComment(s), nil
}

func applyEntry(c *Config, e yamlEntry) error {
	// str returns the entry as one string, rejecting lists and empty blocks.
	str := func() (string, error) {
		if e.isList {
			if len(e.list) == 0 {
				return "", fmt.Errorf("%s needs a value", e.key)
			}
			return "", fmt.Errorf("%s takes a single value, not a list", e.key)
		}
		return e.value, nil
	}
	dur := func(dst *time.Duration) error {
		s, err := str()
		if err != nil {
			return err
		}
		d, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("%s: %w", e.key, err)
		}
		*dst = d
		return nil
	}
	set := func(dst *string) error {
		s, err := str()
		if err != nil {
			return err
		}
		*dst = s
		return nil
	}

	switch e.key {
	case "listen":
		return set(&c.Listen)
	case "advertise":
		return set(&c.Advertise)
	case "data-dir":
		return set(&c.DataDir)
	case "http-addr":
		return set(&c.HTTPAddr)
	case "log-level":
		return set(&c.LogLevel)
	case "log-format":
		return set(&c.LogFormat)
	case "peers":
		if e.isList {
			c.Peers = append([]string(nil), e.list...)
		} else {
			c.Peers = splitList(e.value)
		}
		return nil
	case "snapshot-threshold":
		s, err := str()
		if err != nil {
			return err
		}
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return fmt.Errorf("snapshot-threshold: %w", err)
		}
		c.SnapshotThreshold = n
		return nil
	case "election-timeout-min":
		return dur(&c.ElectionTimeoutMin)
	case "election-timeout-max":
		return dur(&c.ElectionTimeoutMax)
	case "heartbeat-interval":
		return dur(&c.HeartbeatInterval)
	case "shutdown-grace":
		return dur(&c.ShutdownGrace)
	default:
		return fmt.Errorf("unknown key %q", e.key)
	}
}
