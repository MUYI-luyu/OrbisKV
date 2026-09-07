package raft

import (
	"net"
	"net/rpc"
	"sync"
	"testing"
	"time"

	"kvraft/pkg/persister"
)

type raftRPCSwitch struct {
	mu   sync.RWMutex
	node *Raft
}

func (s *raftRPCSwitch) set(node Node) {
	s.mu.Lock()
	s.node = node.(*Raft)
	s.mu.Unlock()
}

func (s *raftRPCSwitch) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	s.mu.RLock()
	node := s.node
	s.mu.RUnlock()
	return node.RequestVote(args, reply)
}
func (s *raftRPCSwitch) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	s.mu.RLock()
	node := s.node
	s.mu.RUnlock()
	return node.AppendEntries(args, reply)
}
func (s *raftRPCSwitch) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	s.mu.RLock()
	node := s.node
	s.mu.RUnlock()
	return node.InstallSnapshot(args, reply)
}

type raftRegressionCluster struct {
	nodes      []Node
	listeners  []net.Listener
	persisters []*persister.FilePersister
	endpoints  []*raftRPCSwitch
	applyChans []chan ApplyMsg
	addrs      []string
}

func newRegressionCluster(t *testing.T, n int) *raftRegressionCluster {
	t.Helper()
	addrs := make([]string, n)
	listeners := make([]net.Listener, n)
	for i := range listeners {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = l
		addrs[i] = l.Addr().String()
	}
	c := &raftRegressionCluster{listeners: listeners, nodes: make([]Node, n), persisters: make([]*persister.FilePersister, n), endpoints: make([]*raftRPCSwitch, n), applyChans: make([]chan ApplyMsg, n), addrs: addrs}
	chans := c.applyChans
	for i := 0; i < n; i++ {
		ps, err := persister.NewFilePersister(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		c.persisters[i] = ps
		chans[i] = make(chan ApplyMsg, 32)
		c.nodes[i] = Make(addrs, i, ps, chans[i])
		c.endpoints[i] = &raftRPCSwitch{}
		c.endpoints[i].set(c.nodes[i])
	}
	for i, l := range listeners {
		srv := rpc.NewServer()
		if err := srv.RegisterName("Raft", c.endpoints[i]); err != nil {
			t.Fatal(err)
		}
		go func(l net.Listener, s *rpc.Server) {
			for {
				conn, err := l.Accept()
				if err != nil {
					return
				}
				go s.ServeConn(conn)
			}
		}(l, srv)
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.Kill()
		}
		for _, l := range c.listeners {
			_ = l.Close()
		}
		for _, ps := range c.persisters {
			_ = ps.Close()
		}
	})
	return c
}

func (c *raftRegressionCluster) restartNode(i int) {
	c.nodes[i].Kill()
	ch := make(chan ApplyMsg, 32)
	c.applyChans[i] = ch
	c.nodes[i] = Make(c.addrs, i, c.persisters[i], ch)
	c.endpoints[i].set(c.nodes[i])
}

func waitLeader(t *testing.T, nodes []Node, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leader := -1
		for i, n := range nodes {
			if _, ok := n.GetState(); ok {
				if leader != -1 {
					leader = -2
					break
				}
				leader = i
			}
		}
		if leader >= 0 {
			return leader
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("leader election timeout")
	return -1
}
func waitAppliedIndex(t *testing.T, n Node, index int) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if n.GetLastApplied() >= index {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("index %d not applied, last=%d", index, n.GetLastApplied())
}

func TestRaftSnapshotMetadataRecoveryRegression(t *testing.T) {
	dir := t.TempDir()
	ps, err := persister.NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	rf := Make([]string{"127.0.0.1:0"}, 0, ps, make(chan ApplyMsg, 4)).(*Raft)
	rf.mu.Lock()
	rf.CurrentTerm = 3
	rf.log = append(rf.log, LogEntry{Term: 3, Command: "snapshot-command"})
	rf.CommitIndex = 1
	rf.persist(rfSnapshotNil())
	rf.mu.Unlock()
	rf.Snapshot(1, []byte("raft-metadata-snapshot"))
	rf.Kill()
	_ = ps.Close()
	ps2, err := persister.NewFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	rf2 := Make([]string{"127.0.0.1:0"}, 0, ps2, make(chan ApplyMsg, 4)).(*Raft)
	defer func() { rf2.Kill(); _ = ps2.Close() }()
	if got := rf2.GetLastIncludedIndex(); got != 1 {
		t.Fatalf("snapshot index=%d want=1", got)
	}
	if got := ps2.ReadSnapshot(); string(got) != "raft-metadata-snapshot" {
		t.Fatalf("snapshot payload=%q", got)
	}
}

func rfSnapshotNil() []byte { return nil }

func TestSingleNodeElectionRegression(t *testing.T) {
	c := newRegressionCluster(t, 1)
	leader := waitLeader(t, c.nodes, 2*time.Second)
	if leader != 0 {
		t.Fatalf("single node leader=%d", leader)
	}
}

func TestThreeNodeReplicationAndFailoverRegression(t *testing.T) {
	c := newRegressionCluster(t, 3)
	leader := waitLeader(t, c.nodes, 3*time.Second)
	idx, term, ok := c.nodes[leader].Start("first")
	if !ok || idx != 1 {
		t.Fatalf("Start index=%d term=%d leader=%v", idx, term, ok)
	}
	for _, n := range c.nodes {
		waitAppliedIndex(t, n, idx)
	}
	_ = c.listeners[leader].Close()
	c.nodes[leader].Kill()
	newLeader := waitLeaderExcluding(t, c.nodes, leader, 5*time.Second)
	idx2, term2, ok := c.nodes[newLeader].Start("second")
	if !ok || idx2 <= idx {
		t.Fatalf("failover Start index=%d term=%d leader=%v", idx2, term2, ok)
	}
	for i, n := range c.nodes {
		if i != leader {
			waitAppliedIndex(t, n, idx2)
		}
	}
}
func waitLeaderExcluding(t *testing.T, nodes []Node, excluded int, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leader := -1
		for i, n := range nodes {
			if i == excluded {
				continue
			}
			if _, ok := n.GetState(); ok {
				if leader != -1 {
					leader = -2
					break
				}
				leader = i
			}
		}
		if leader >= 0 {
			return leader
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatal("failover election timeout")
	return -1
}

func TestThreeNodeCrashRestartRecoversAndContinuesRegression(t *testing.T) {
	c := newRegressionCluster(t, 3)
	leader := waitLeader(t, c.nodes, 3*time.Second)
	idx, _, ok := c.nodes[leader].Start("before-restart")
	if !ok {
		t.Fatal("leader rejected initial command")
	}
	for _, node := range c.nodes {
		waitAppliedIndex(t, node, idx)
	}

	follower := (leader + 1) % len(c.nodes)
	c.restartNode(follower)
	restored := c.nodes[follower].(*Raft)
	restored.mu.RLock()
	restoredLastLog := restored.getLastLogIndex()
	restored.mu.RUnlock()
	if restoredLastLog < idx {
		t.Fatalf("restarted follower lost persisted log: last=%d want>=%d", restoredLastLog, idx)
	}
	waitAppliedIndex(t, c.nodes[follower], idx)

	idx2, _, ok := c.nodes[leader].Start("after-follower-restart")
	if !ok {
		t.Fatal("leader rejected command after follower restart")
	}
	for _, node := range c.nodes {
		waitAppliedIndex(t, node, idx2)
	}

	c.restartNode(leader)
	newLeader := waitLeader(t, c.nodes, 5*time.Second)
	idx3, _, ok := c.nodes[newLeader].Start("after-leader-restart")
	if !ok {
		t.Fatal("cluster rejected command after leader restart")
	}
	for _, node := range c.nodes {
		waitAppliedIndex(t, node, idx3)
	}
}
