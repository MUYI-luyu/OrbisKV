package wal

import (
	"path/filepath"
	"testing"
)

func TestWALAppendCloseReopenReplayRegression(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rsm.log")
	logger, err := NewLogger(path, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := logger.Replay(func(Entry) error { return nil }); err != nil {
		t.Fatal(err)
	}
	want := Entry{RaftIndex: 7, Term: 3, NodeID: 1, ReqID: 9, OpType: "COMMIT_TX", Key: "tx-7", TxWriteKeys: []string{"k"}, TxWriteValues: []string{"v"}, TxWriteVersions: []int64{2}, TxWriteDeletes: []bool{false}}
	if err := logger.Append(want); err != nil {
		t.Fatal(err)
	}
	if err := logger.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewLogger(path, true, true)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	var got []Entry
	if err := reopened.Replay(func(entry Entry) error { got = append(got, entry); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].OpType != want.OpType || len(got[0].TxWriteKeys) != 1 || got[0].TxWriteKeys[0] != "k" || got[0].TxWriteVersions[0] != 2 {
		t.Fatalf("replayed WAL=%+v", got)
	}
}
