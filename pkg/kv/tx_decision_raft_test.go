package kv

import (
	"encoding/gob"
	"net"
	"net/rpc"
	"testing"
	"time"

	"kvraft/pkg/persister"
	"kvraft/pkg/storage"
)

func TestCoordinatorDecisionSurvivesRaftLeaderFailover(t *testing.T) {
	t.Setenv("KV_DATA_DIR", t.TempDir())
	t.Setenv("KV_WAL_ENABLED", "false")
	gob.Register(Op{})
	gob.Register(RecordTxDecisionArgs{})
	gob.Register(ResolveTxStatusArgs{})
	const n = 3
	listeners := make([]net.Listener, n)
	addrs := make([]string, n)
	for i := range listeners {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i], addrs[i] = l, l.Addr().String()
	}
	rsms := make([]*RSM, n)
	stores := make([]*storage.Store, n)
	persisters := make([]*persister.FilePersister, n)
	for i := 0; i < n; i++ {
		store, err := storage.NewStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		ps, err := persister.NewFilePersister(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		kv := NewKVServer(i, 1, addrs[i], store)
		rsm := MakeRSM(addrs, i, ps, -1, kv)
		kv.SetRSM(rsm)
		stores[i], persisters[i], rsms[i] = store, ps, rsm
		srv := rpc.NewServer()
		if err := srv.RegisterName("Raft", rsm.rf); err != nil {
			t.Fatal(err)
		}
		go func(l net.Listener, srv *rpc.Server) {
			for {
				conn, err := l.Accept()
				if err != nil {
					return
				}
				go srv.ServeConn(conn)
			}
		}(listeners[i], srv)
	}
	defer func() {
		for _, rsm := range rsms {
			rsm.Close()
		}
		for _, l := range listeners {
			_ = l.Close()
		}
		for _, s := range stores {
			_ = s.Close()
		}
		for _, ps := range persisters {
			_ = ps.Close()
		}
	}()
	leader := waitRSMLeader(t, rsms, nil)
	errCode, value := rsms[leader].Submit(&RecordTxDecisionArgs{TxID: "leader-failover", Decision: TxStatusCommitted, ParticipantGroupIDs: []int{1, 2}})
	if errCode != OK || value.(RecordTxDecisionReply).Err != OK {
		t.Fatalf("record decision: %s/%v", errCode, value)
	}
	rsms[leader].Close()
	newLeader := waitRSMLeader(t, rsms, map[int]bool{leader: true})
	errCode, value = rsms[newLeader].Submit(&ResolveTxStatusArgs{TxID: "leader-failover"})
	if errCode != OK {
		t.Fatalf("resolve after failover: %s", errCode)
	}
	status := value.(ResolveTxStatusReply)
	if status.Status != TxStatusCommitted || len(status.ParticipantGroupIDs) != 2 {
		t.Fatalf("status=%+v", status)
	}
}

func waitRSMLeader(t *testing.T, rsms []*RSM, skip map[int]bool) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		leader := -1
		for i, rsm := range rsms {
			if skip[i] {
				continue
			}
			if _, ok := rsm.GetState(); ok {
				leader = i
				break
			}
		}
		if leader >= 0 {
			return leader
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("RSM leader election timeout")
	return -1
}
