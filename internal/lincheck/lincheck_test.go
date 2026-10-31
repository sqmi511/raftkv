package lincheck

import (
	"errors"
	"strings"
	"testing"
)

func wset(key, v string, call, ret int64) Op {
	return Op{Kind: Set, Key: key, Value: v, Call: call, Return: ret}
}

func rget(key string, found bool, v string, call, ret int64) Op {
	return Op{Kind: Get, Key: key, Found: found, Got: v, Call: call, Return: ret}
}

func del(key string, deleted bool, call, ret int64) Op {
	return Op{Kind: Delete, Key: key, Found: deleted, Call: call, Return: ret}
}

func unknownSet(key, v string, call int64) Op {
	return Op{Kind: Set, Key: key, Value: v, Call: call, Unknown: true}
}

func TestLinearizableHistories(t *testing.T) {
	cases := map[string][]Op{
		"empty": nil,
		"sequential": {
			wset("a", "1", 1, 2), rget("a", true, "1", 3, 4), del("a", true, 5, 6), rget("a", false, "", 7, 8),
		},
		"read of an untouched key": {rget("a", false, "", 1, 2)},
		"delete of a missing key":  {del("a", false, 1, 2)},
		"read overlapping a write may see either side": {
			wset("a", "1", 1, 10), rget("a", false, "", 2, 3), rget("a", true, "1", 4, 5),
		},
		"concurrent writes, either order": {
			wset("a", "1", 1, 10), wset("a", "2", 1, 10), rget("a", true, "1", 11, 12),
		},
		"concurrent writes, the other order": {
			wset("a", "1", 1, 10), wset("a", "2", 1, 10), rget("a", true, "2", 11, 12),
		},
		"touching intervals count as concurrent": {
			wset("a", "1", 1, 5), rget("a", false, "", 5, 6),
		},
	}
	for name, ops := range cases {
		if res := Check(ops); !res.OK || res.Err != nil {
			t.Errorf("%s: %+v, want linearizable", name, res)
		}
	}
}

func TestViolationsAreDetected(t *testing.T) {
	cases := map[string][]Op{
		"stale read": {
			wset("a", "1", 1, 2), wset("a", "2", 3, 4), rget("a", true, "1", 5, 6),
		},
		"lost write": {
			wset("a", "1", 1, 2), rget("a", false, "", 3, 4),
		},
		"read from the future": {
			rget("a", true, "1", 1, 2), wset("a", "1", 3, 4),
		},
		"values flip-flop": {
			wset("a", "1", 1, 2), wset("a", "2", 3, 4), rget("a", true, "2", 5, 6), rget("a", true, "1", 7, 8),
		},
		"concurrent writes observed in both orders": {
			wset("a", "1", 1, 10), wset("a", "2", 1, 10), rget("a", true, "1", 11, 12), rget("a", true, "2", 13, 14),
		},
		"delete claims a key that never existed": {del("a", true, 1, 2)},
		"delete denies a key that exists": {
			wset("a", "1", 1, 2), del("a", false, 3, 4),
		},
		"deleted key reappears": {
			wset("a", "1", 1, 2), del("a", true, 3, 4), rget("a", true, "1", 5, 6),
		},
	}
	for name, ops := range cases {
		res := Check(ops)
		if res.OK || res.Err != nil || res.Key != "a" {
			t.Errorf("%s: %+v, want a violation on key a", name, res)
		}
	}
}

func TestIndeterminateOperations(t *testing.T) {
	t.Run("may have taken effect", func(t *testing.T) {
		ops := []Op{unknownSet("a", "1", 1), rget("a", true, "1", 5, 6)}
		if res := Check(ops); !res.OK {
			t.Fatalf("%+v, want linearizable", res)
		}
	})
	t.Run("may never have taken effect", func(t *testing.T) {
		ops := []Op{unknownSet("a", "1", 1), rget("a", false, "", 5, 6)}
		if res := Check(ops); !res.OK {
			t.Fatalf("%+v, want linearizable", res)
		}
	})
	t.Run("may take effect arbitrarily late", func(t *testing.T) {
		ops := []Op{unknownSet("a", "1", 1), rget("a", false, "", 5, 6), rget("a", true, "1", 50, 60)}
		if res := Check(ops); !res.OK {
			t.Fatalf("%+v, want linearizable", res)
		}
	})
	t.Run("cannot take effect before it started", func(t *testing.T) {
		ops := []Op{rget("a", true, "1", 1, 2), unknownSet("a", "1", 5)}
		if res := Check(ops); res.OK || res.Err != nil {
			t.Fatalf("%+v, want a violation", res)
		}
	})
	t.Run("a value cannot vanish on its own", func(t *testing.T) {
		ops := []Op{unknownSet("a", "1", 1), rget("a", true, "1", 2, 3), rget("a", false, "", 4, 5)}
		if res := Check(ops); res.OK || res.Err != nil {
			t.Fatalf("%+v, want a violation", res)
		}
	})
	t.Run("an unknown delete explains a missing key", func(t *testing.T) {
		ops := []Op{
			wset("a", "1", 1, 2),
			{Kind: Delete, Key: "a", Call: 3, Unknown: true},
			rget("a", false, "", 10, 11),
		}
		if res := Check(ops); !res.OK {
			t.Fatalf("%+v, want linearizable", res)
		}
	})
	t.Run("unknown reads are ignored", func(t *testing.T) {
		ops := []Op{wset("a", "1", 1, 2), {Kind: Get, Key: "a", Call: 3, Unknown: true}}
		if res := Check(ops); !res.OK {
			t.Fatalf("%+v, want linearizable", res)
		}
	})
}

func TestKeysAreIndependent(t *testing.T) {
	ops := []Op{
		wset("a", "1", 1, 2), rget("a", true, "1", 3, 4), // fine
		wset("b", "1", 1, 2), rget("b", false, "", 3, 4), // lost write on b only
	}
	if res := Check(ops); res.OK || res.Key != "b" {
		t.Fatalf("%+v, want a violation on key b", res)
	}
}

func TestBudgetAndInvalidInput(t *testing.T) {
	ops := []Op{wset("a", "1", 1, 2), rget("a", true, "1", 3, 4), rget("a", true, "1", 5, 6)}

	res := CheckBudget(ops, 1)
	if res.OK || !errors.Is(res.Err, ErrBudget) {
		t.Fatalf("%+v, want ErrBudget", res)
	}
	if res := CheckBudget(ops, 1000); !res.OK {
		t.Fatalf("%+v, want linearizable with a normal budget", res)
	}

	if res := Check([]Op{wset("a", "1", 5, 2)}); !errors.Is(res.Err, ErrInvalidOp) {
		t.Fatalf("a request that returns before it starts: %+v, want ErrInvalidOp", res)
	}
	if res := Check([]Op{{Kind: 0, Key: "a"}}); !errors.Is(res.Err, ErrInvalidOp) {
		t.Fatalf("an unknown kind: %+v, want ErrInvalidOp", res)
	}
}

func TestManyConcurrentWritersStayFast(t *testing.T) {
	var ops []Op
	for i := 0; i < 12; i++ {
		ops = append(ops, wset("a", string(rune('a'+i)), 1, 100))
	}
	ops = append(ops, rget("a", true, "g", 101, 102))
	if res := Check(ops); !res.OK {
		t.Fatalf("%+v, want linearizable", res)
	}
}

func TestFormat(t *testing.T) {
	ops := []Op{
		{ClientID: 2, Kind: Get, Key: "a", Found: true, Got: "x", Call: 2000, Return: 3000},
		{ClientID: 1, Kind: Set, Key: "a", Value: "x", Call: 1000, Return: 1500},
		{ClientID: 3, Kind: Delete, Key: "a", Call: 4000, Unknown: true},
		{ClientID: 1, Kind: Set, Key: "other", Value: "zzz", Call: 1, Return: 2},
	}
	out := Format(ops, "a")
	if strings.Contains(out, "zzz") {
		t.Fatalf("Format included another key:\n%s", out)
	}
	set, get, del := strings.Index(out, `Set("x")`), strings.Index(out, `Get -> "x"`), strings.Index(out, "Delete -> unknown")
	if set < 0 || get < set || del < get {
		t.Fatalf("operations are missing or not in start order:\n%s", out)
	}
	if !strings.Contains(out, ".. ?]") {
		t.Fatalf("an unknown operation should show '?' as its end:\n%s", out)
	}
}
