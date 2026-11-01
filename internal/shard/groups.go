package shard

import (
	"fmt"
	"strings"
)

// Group is one raftkv cluster: a name and the client addresses of its nodes.
type Group struct {
	Name  string
	Addrs []string
}

// ParseGroups reads a group list such as
//
//	"users=10.0.0.1:7000,10.0.0.2:7000;orders=10.0.1.1:7000"
//
// Groups are separated by ";", a name is separated from its addresses by "=",
// and addresses by ",". Every client of the same data must use the same
// group names (the names, not the addresses, decide where keys live).
func ParseGroups(spec string) ([]Group, error) {
	var out []Group
	seen := make(map[string]bool)
	for _, part := range strings.Split(spec, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, list, ok := strings.Cut(part, "=")
		name = strings.TrimSpace(name)
		if !ok || name == "" {
			return nil, fmt.Errorf("%w: %q: want name=addr,addr", ErrInvalidGroup, part)
		}
		if seen[name] {
			return nil, fmt.Errorf("%w: %q appears twice", ErrInvalidGroup, name)
		}
		var addrs []string
		for _, a := range strings.Split(list, ",") {
			if a = strings.TrimSpace(a); a != "" {
				addrs = append(addrs, a)
			}
		}
		if len(addrs) == 0 {
			return nil, fmt.Errorf("%w: %q has no addresses", ErrInvalidGroup, name)
		}
		seen[name] = true
		out = append(out, Group{Name: name, Addrs: addrs})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: no groups given", ErrInvalidGroup)
	}
	return out, nil
}
