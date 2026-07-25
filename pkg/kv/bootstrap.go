package kv

import (
	"encoding/gob"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"

	"kvraft/pkg/raft"
	"kvraft/pkg/storage"
)

func runtimeDataRoot() string {
	v := strings.TrimSpace(os.Getenv("KV_DATA_DIR"))
	if v != "" {
		return v
	}
	if info, err := os.Stat("/data"); err == nil && info.IsDir() {
		return "/data"
	}
	return "data"
}

func leaseStatsEnabledFromEnv() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("KV_LEASE_STATS")))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func StartKVServer(servers []string, gid int, me int, persister raft.Persister, maxraftstate int, address string) *KVServer {
	gob.Register(Op{})
	gob.Register(PutArgs{})
	gob.Register(GetArgs{})
	gob.Register(DeleteArgs{})
	gob.Register(ScanArgs{})
	gob.Register(ExpireArgs{})
	gob.Register(PrepareTxArgs{})
	gob.Register(CommitTxArgs{})
	gob.Register(AbortTxArgs{})
	gob.Register(ResolveTxStatusArgs{})

	storePath := filepath.Join(runtimeDataRoot(), "badger-"+address)
	store, err := storage.NewStore(storePath)
	if err != nil {
		log.Fatal(err)
	}

	kvServer := NewKVServer(me, gid, address, store)
	rsm := MakeRSM(servers, me, persister, maxraftstate, kvServer)
	kvServer.SetRSM(rsm)
	rsm.RegisterOpCompleteListener(kvServer)

	if err := kvServer.txMgr.RebuildLockTable(); err != nil {
		log.Printf("[bootstrap] RebuildLockTable error: %v", err)
	}

	rpcs := rpc.NewServer()
	if err := rpcs.RegisterName("Raft", rsm.rf); err != nil {
		log.Fatal(err)
	}
	l, e := net.Listen("tcp", address)
	if e != nil {
		log.Fatal(e)
	}
	kvServer.SetRPCListener(l)

	go func() {
		for kvServer.IsAlive() {
			conn, err := l.Accept()
			if err == nil && kvServer.IsAlive() {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
		}
		l.Close()
	}()

	StartGRPCServer(kvServer, address)
	go kvServer.TTLCleanupLoop()

	return kvServer
}
