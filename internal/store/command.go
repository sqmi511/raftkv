package store

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Op identifies the kind of a Command.
type Op byte

const (
	OpSet    Op = 1
	OpDelete Op = 2
)

// ErrBadCommand is returned when a serialized command cannot be decoded.
var ErrBadCommand = errors.New("store: malformed command")

// Command is one state-changing operation. It is what gets written to the
// WAL (and, later, to the Raft log).
//
// Wire format:
//
//	op (1B) | expiresAt unix-nanos, big endian (8B) | uvarint(len(key)) | key | value
//
// ExpiresAt is an absolute time (0 = never), so replaying a command after a
// restart never extends a key's lifetime.
type Command struct {
	Op        Op
	Key       string
	Value     []byte
	ExpiresAt int64
}

const minCommandSize = 1 + 8 + 1

// Encode serializes the command.
func (c Command) Encode() []byte {
	buf := make([]byte, 0, 1+8+binary.MaxVarintLen64+len(c.Key)+len(c.Value))
	buf = append(buf, byte(c.Op))
	buf = binary.BigEndian.AppendUint64(buf, uint64(c.ExpiresAt))
	buf = binary.AppendUvarint(buf, uint64(len(c.Key)))
	buf = append(buf, c.Key...)
	return append(buf, c.Value...)
}

// DecodeCommand parses a serialized command. The result does not alias b.
func DecodeCommand(b []byte) (Command, error) {
	if len(b) < minCommandSize {
		return Command{}, fmt.Errorf("%w: too short (%d bytes)", ErrBadCommand, len(b))
	}
	op := Op(b[0])
	if op != OpSet && op != OpDelete {
		return Command{}, fmt.Errorf("%w: unknown op %d", ErrBadCommand, b[0])
	}
	expiresAt := int64(binary.BigEndian.Uint64(b[1:9]))

	klen, n := binary.Uvarint(b[9:])
	if n <= 0 {
		return Command{}, fmt.Errorf("%w: bad key length", ErrBadCommand)
	}
	rest := b[9+n:]
	if klen > uint64(len(rest)) {
		return Command{}, fmt.Errorf("%w: key length %d exceeds record", ErrBadCommand, klen)
	}

	c := Command{Op: op, ExpiresAt: expiresAt, Key: string(rest[:klen])}
	if v := rest[klen:]; len(v) > 0 {
		c.Value = append([]byte(nil), v...)
	}
	return c, nil
}
