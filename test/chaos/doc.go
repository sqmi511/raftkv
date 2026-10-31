// Package chaos holds the chaos tests: randomized fault injection against a
// five-node cluster, with the recorded history checked for linearizability.
// Run them with: go test -race -v ./test/chaos -chaos.duration=30s
package chaos
