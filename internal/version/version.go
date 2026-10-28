// Package version exposes build metadata, injected at link time via -ldflags.
package version

// Version is overridden at build time:
//
//	go build -ldflags "-X github.com/sqmi511/raftkv/internal/version.Version=v0.1.0"
var Version = "dev"

// String returns a human-readable version string.
func String() string {
	return "raftkv " + Version
}