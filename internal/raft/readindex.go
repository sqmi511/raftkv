package raft

import (
	"context"
	"time"
)

// readRequest is one ReadIndex call waiting for a quorum to confirm that we
// are still the leader.
type readRequest struct {
	seq   uint64 // position in the stream of read requests
	index uint64 // commit index when the request arrived
	done  chan error
}

// ReadIndex is the first half of a linearizable read. It returns a log index
// such that, once the caller's state machine has applied everything up to
// that index, a local read reflects every write that completed before
// ReadIndex was called.
//
// How it works: the leader records its commit index, then confirms it is
// still leader by collecting acknowledgements from a majority for
// AppendEntries RPCs sent AFTER the request arrived. A leader cut off from
// the majority therefore blocks (until ctx ends or it learns of a newer
// term and returns ErrNotLeader) instead of returning stale data. Reads that
// arrive together share the same round of heartbeats.
//
// Errors: ErrNotLeader on a follower (or after losing leadership),
// ErrNotReady right after an election (retry shortly), ErrStopped, or ctx's error.
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	r, err := n.beginRead()
	if err != nil {
		return 0, err
	}
	select {
	case err := <-r.done:
		if err != nil {
			return 0, err
		}
		return r.index, nil
	case <-ctx.Done():
		n.mu.Lock()
		n.removeRead(r)
		n.mu.Unlock()
		return 0, ctx.Err()
	case <-n.ctx.Done():
		return 0, ErrStopped
	}
}

func (n *Node) beginRead() (*readRequest, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	switch {
	case n.stopped:
		return nil, ErrStopped
	case n.failed != nil:
		return nil, n.failed
	case n.state != Leader:
		return nil, ErrNotLeader
	case n.commitIndex < n.termStart:
		// Until an entry of our own term is committed, our commit index may
		// lag behind what the previous leader already committed.
		return nil, ErrNotReady
	}

	n.readSeq++
	r := &readRequest{seq: n.readSeq, index: n.commitIndex, done: make(chan error, 1)}
	n.reads = append(n.reads, r)
	n.broadcast(time.Now())
	n.completeReads() // a single-node cluster is confirmed right here
	return r, nil
}

// completeReads releases every read that a quorum has confirmed.
func (n *Node) completeReads() {
	if len(n.reads) == 0 {
		return
	}
	remaining := n.reads[:0]
	for _, r := range n.reads {
		acks := 1 // ourselves
		for _, p := range n.progress {
			if p.ackSeq >= r.seq {
				acks++
			}
		}
		if acks >= n.quorum() {
			r.done <- nil
		} else {
			remaining = append(remaining, r)
		}
	}
	n.reads = remaining
}

// failReads aborts every pending read with err.
func (n *Node) failReads(err error) {
	for _, r := range n.reads {
		r.done <- err
	}
	n.reads = nil
}

func (n *Node) removeRead(r *readRequest) {
	for i, x := range n.reads {
		if x == r {
			n.reads = append(n.reads[:i], n.reads[i+1:]...)
			return
		}
	}
}
