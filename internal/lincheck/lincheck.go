// Package lincheck decides whether a recorded history of key-value
// operations is linearizable: whether the operations can be arranged in one
// sequential order that (a) respects real time (an operation that finished
// before another started comes first) and (b) behaves like a plain map.
//
// It is the Wing-Gong search with memoization (as popularised by Lowe),
// applied to each key separately, which is sound because linearizability is
// compositional. The worst case is exponential, so the search has a budget.
//
// Operations whose outcome the client never learned (timeouts, errors) are
// marked Unknown. They may take effect at any moment after they started, or
// never; their returned value is not checked.
package lincheck

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

var (
	// ErrBudget means the search gave up: the result is inconclusive.
	ErrBudget = errors.New("lincheck: search budget exceeded (result inconclusive)")
	// ErrInvalidOp means the history itself is malformed.
	ErrInvalidOp = errors.New("lincheck: invalid operation")
)

// DefaultBudget is the number of search states explored per key.
const DefaultBudget = 20_000_000

// Kind is the type of an operation.
type Kind uint8

const (
	Set Kind = iota + 1
	Delete
	Get
)

func (k Kind) String() string {
	switch k {
	case Set:
		return "Set"
	case Delete:
		return "Delete"
	case Get:
		return "Get"
	}
	return fmt.Sprintf("Kind(%d)", uint8(k))
}

// Op is one client operation. Times are nanoseconds on any shared clock.
type Op struct {
	ClientID int
	Kind     Kind
	Key      string

	Value string // Set: the value written
	Found bool   // Get: the key was found; Delete: a live key was removed
	Got   string // Get: the value read (when Found)

	Call   int64 // when the client started the operation
	Return int64 // when the client got the answer

	// Unknown means the client never learned the outcome. Return and the
	// results (Found, Got) are ignored. An unknown Get has no effect and is
	// dropped.
	Unknown bool
}

// Result is the outcome of a check.
type Result struct {
	OK  bool
	Key string // the key whose history failed (when !OK)
	// Err is set when the check could not be completed: the history is
	// malformed or the budget ran out. OK is false then, but that does not
	// prove a violation.
	Err error
}

// Check verifies ops with DefaultBudget.
func Check(ops []Op) Result { return CheckBudget(ops, DefaultBudget) }

// CheckBudget verifies ops, exploring at most budget states per key.
func CheckBudget(ops []Op, budget int) Result {
	byKey := make(map[string][]Op)
	for _, op := range ops {
		switch {
		case op.Kind < Set || op.Kind > Get:
			return Result{Err: fmt.Errorf("%w: unknown kind %d", ErrInvalidOp, op.Kind)}
		case !op.Unknown && op.Return < op.Call:
			return Result{Key: op.Key, Err: fmt.Errorf("%w: %s on %q returns before it is called", ErrInvalidOp, op.Kind, op.Key)}
		case op.Kind == Get && op.Unknown:
			continue
		}
		byKey[op.Key] = append(byKey[op.Key], op)
	}

	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		ok, err := checkKey(byKey[k], budget)
		if err != nil {
			return Result{Key: k, Err: fmt.Errorf("key %q: %w", k, err)}
		}
		if !ok {
			return Result{Key: k}
		}
	}
	return Result{OK: true}
}

// regState is the state of one key: a register that may be absent.
type regState struct {
	present bool
	value   string
}

// step applies op to st. ok is false if op's recorded result is impossible
// in state st.
func step(st regState, op Op) (next regState, ok bool) {
	switch op.Kind {
	case Set:
		return regState{present: true, value: op.Value}, true
	case Delete:
		if !op.Unknown && op.Found != st.present {
			return st, false
		}
		return regState{}, true
	case Get:
		if op.Found != st.present || (op.Found && op.Got != st.value) {
			return st, false
		}
		return st, true
	}
	return st, false
}

type searcher struct {
	ops       []Op // sorted by Call
	ret       []int64
	remaining []uint64 // bitset of operations not yet linearized
	mandatory int      // remaining operations that are not Unknown
	seen      map[string]struct{}
	steps     int
	budget    int
}

func checkKey(ops []Op, budget int) (bool, error) {
	sort.SliceStable(ops, func(i, j int) bool { return ops[i].Call < ops[j].Call })
	s := &searcher{
		ops:       ops,
		ret:       make([]int64, len(ops)),
		remaining: make([]uint64, (len(ops)+63)/64),
		seen:      make(map[string]struct{}),
		budget:    budget,
	}
	for i, op := range ops {
		s.ret[i] = op.Return
		if op.Unknown {
			s.ret[i] = math.MaxInt64 // may take effect arbitrarily late
		} else {
			s.mandatory++
		}
		s.remaining[i/64] |= 1 << (uint(i) % 64)
	}
	return s.dfs(regState{})
}

func (s *searcher) has(i int) bool { return s.remaining[i/64]>>(uint(i)%64)&1 == 1 }

func (s *searcher) take(i int) {
	s.remaining[i/64] &^= 1 << (uint(i) % 64)
	if !s.ops[i].Unknown {
		s.mandatory--
	}
}

func (s *searcher) put(i int) {
	s.remaining[i/64] |= 1 << (uint(i) % 64)
	if !s.ops[i].Unknown {
		s.mandatory++
	}
}

// key identifies a search state: which operations remain, and the register.
func (s *searcher) key(st regState) string {
	b := make([]byte, 0, len(s.remaining)*8+1+len(st.value))
	for _, w := range s.remaining {
		b = binary.LittleEndian.AppendUint64(b, w)
	}
	if st.present {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	return string(append(b, st.value...))
}

// dfs reports whether the remaining operations can be linearized from st.
// Unknown operations left over at the end are simply treated as "never took
// effect".
func (s *searcher) dfs(st regState) (bool, error) {
	if s.mandatory == 0 {
		return true, nil
	}
	k := s.key(st)
	if _, dead := s.seen[k]; dead {
		return false, nil
	}
	if s.steps++; s.steps > s.budget {
		return false, ErrBudget
	}

	// An operation can go next only if no other remaining operation
	// finished before it started.
	minRet := int64(math.MaxInt64)
	for i := range s.ops {
		if s.has(i) && s.ret[i] < minRet {
			minRet = s.ret[i]
		}
	}

	for i := range s.ops {
		if !s.has(i) {
			continue
		}
		if s.ops[i].Call > minRet {
			break // ops are sorted by Call: no later one qualifies either
		}
		next, ok := step(st, s.ops[i])
		if !ok {
			continue
		}
		s.take(i)
		good, err := s.dfs(next)
		s.put(i)
		if err != nil {
			return false, err
		}
		if good {
			return true, nil
		}
	}
	s.seen[k] = struct{}{}
	return false, nil
}

// Format renders the operations on key in start order, for failure reports.
func Format(ops []Op, key string) string {
	var list []Op
	for _, op := range ops {
		if op.Key == key {
			list = append(list, op)
		}
	}
	sort.SliceStable(list, func(i, j int) bool { return list[i].Call < list[j].Call })

	at := func(ns int64) string { return time.Duration(ns).Round(time.Microsecond).String() }
	var b strings.Builder
	for _, op := range list {
		end := "?"
		if !op.Unknown {
			end = at(op.Return)
		}
		var what string
		switch op.Kind {
		case Set:
			what = fmt.Sprintf("Set(%q)", op.Value)
			if op.Unknown {
				what += " -> unknown"
			}
		case Delete:
			what = "Delete -> unknown"
			if !op.Unknown {
				what = fmt.Sprintf("Delete -> deleted=%v", op.Found)
			}
		case Get:
			if op.Found {
				what = fmt.Sprintf("Get -> %q", op.Got)
			} else {
				what = "Get -> not found"
			}
		}
		fmt.Fprintf(&b, "  client %-2d [%s .. %s]  %s\n", op.ClientID, at(op.Call), end, what)
	}
	return b.String()
}
