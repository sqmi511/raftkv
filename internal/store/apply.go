package store

import "time"

// Apply executes one state-changing command. It reports whether a live key
// was removed (only ever true for OpDelete).
//
// ExpiresAt is an absolute time. A Set whose expiry has already passed
// deletes the key instead, so an older value is never resurrected. Unknown
// operations are ignored.
func (s *Store) Apply(c Command) (deleted bool) {
	switch c.Op {
	case OpSet:
		if c.ExpiresAt == 0 {
			s.Set(c.Key, c.Value, 0)
			return false
		}
		remaining := time.Unix(0, c.ExpiresAt).Sub(s.now())
		if remaining <= 0 {
			s.Delete(c.Key)
			return false
		}
		s.Set(c.Key, c.Value, remaining)
	case OpDelete:
		return s.Delete(c.Key)
	}
	return false
}
