package kv

import (
	"sync/atomic"
	"testing"
	"time"

	"kvraft/pkg/persister"
	"kvraft/pkg/raft"
)

type leaseReadStateMachine struct{ calls atomic.Int32 }

func (s *leaseReadStateMachine) DoOp(req any) any { s.calls.Add(1); return req }
func (*leaseReadStateMachine) Snapshot() []byte   { return nil }
func (*leaseReadStateMachine) Restore([]byte)     {}

func newLeaseReadRSM(t *testing.T) (*RSM, *leaseReadStateMachine, raft.Node) {
	t.Helper()
	ps, err := persister.NewFilePersister(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	node := raft.Make([]string{"127.0.0.1:0"}, 0, ps, make(chan raft.ApplyMsg, 4))
	sm := &leaseReadStateMachine{}
	rsm := &RSM{me: 0, rf: node, sm: sm, waitingOps: make(map[int]*waitingOp)}
	rsm.leaseRead.Store(true)
	t.Cleanup(func() { node.Kill() })
	return rsm, sm, node
}

func TestLeaseReadFallsBackWhenLeaseUnavailable(t *testing.T) {
	rsm, sm, _ := newLeaseReadRSM(t)
	err, _, leaseHit := rsm.SubmitLeaseReadWithMode(&GetArgs{Key: "k"})
	if leaseHit {
		t.Fatal("follower must not serve a lease read")
	}
	if err != ErrWrongLeader {
		t.Fatalf("fallback result=%s want ErrWrongLeader", err)
	}
	if sm.calls.Load() != 0 {
		t.Fatal("fallback must not execute the state machine locally")
	}
}

func TestLeaseReadBypassesConsensusWithValidLeaderLease(t *testing.T) {
	rsm, sm, node := newLeaseReadRSM(t)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, leader := node.GetState(); leader && rsm.IsLeaderWithLease() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, leader := node.GetState(); !leader || !rsm.IsLeaderWithLease() {
		t.Fatal("single node did not obtain leader lease")
	}
	req := &GetArgs{Key: "k"}
	err, got, leaseHit := rsm.SubmitLeaseReadWithMode(req)
	if err != OK || !leaseHit {
		t.Fatalf("lease read err=%s hit=%v", err, leaseHit)
	}
	if got != req || sm.calls.Load() != 1 {
		t.Fatalf("local state machine result=%#v calls=%d", got, sm.calls.Load())
	}
	if node.GetLastApplied() != 0 {
		t.Fatalf("lease read unexpectedly entered Raft apply path: lastApplied=%d", node.GetLastApplied())
	}
}
