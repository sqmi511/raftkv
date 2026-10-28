package version

import "testing"

func TestString(t *testing.T) {
	old := Version
	t.Cleanup(func() { Version = old })

	Version = "v1.2.3"
	if got, want := String(), "raftkv v1.2.3"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}
