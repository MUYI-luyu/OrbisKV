package kv

import (
	"testing"

	"kvraft/pkg/persister"
)

func TestMakeRSMEnablesWALByDefault(t *testing.T) {
	t.Setenv("KV_WAL_ENABLED", "")
	t.Setenv("KV_DATA_DIR", t.TempDir())

	ps, err := persister.NewFilePersister(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rsm := MakeRSM([]string{"127.0.0.1:0"}, 0, ps, 0, &leaseReadStateMachine{})
	defer func() {
		rsm.Close()
		_ = ps.Close()
	}()

	if rsm.walLogger == nil || !rsm.walLogger.Enabled() {
		t.Fatal("RSM WAL must be enabled when KV_WAL_ENABLED is unset")
	}
}
