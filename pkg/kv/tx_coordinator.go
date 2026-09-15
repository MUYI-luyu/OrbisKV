package kv

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	pb "kvraft/api/pb/kvraft/api/pb"
	"kvraft/pkg/sharding"
)

const (
	txDefaultTimeoutMs = 10000
	txCommitMaxRetries = 5
)

// TxHandle 是面向用户的单次分布式事务句柄。
// 在本地缓存读写操作，在 Commit() 时执行 2PC 协议。
type TxHandle struct {
	txID        string
	readSet     map[string]ReadKey
	writeSet    map[string]WriteKey
	groups      map[int]bool
	coordinator *TxCoordinator
	timeoutMs   int64
	mu          sync.Mutex
}

// TxCoordinator 在客户端管理 2PC 事务生命周期。
// 嵌入在 Clerk 中，复用现有的 ShardRouter 进行路由。
type TxCoordinator struct {
	router *sharding.ShardRouter
}

// NewTxCoordinator 创建一个新的 TxCoordinator。
func NewTxCoordinator(router *sharding.ShardRouter) *TxCoordinator {
	return &TxCoordinator{router: router}
}

// Begin 开始一个新事务并返回 TxHandle。
func (tc *TxCoordinator) Begin() *TxHandle {
	return &TxHandle{
		txID:        generateTxID(),
		readSet:     make(map[string]ReadKey),
		writeSet:    make(map[string]WriteKey),
		groups:      make(map[int]bool),
		coordinator: tc,
		timeoutMs:   txDefaultTimeoutMs,
	}
}

// generateTxID 生成唯一事务 ID："{unixNano}-{8位随机hex}"。
func generateTxID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), hex.EncodeToString(b))
}

// Get 在事务内读取一个 key。先查 WriteSet（脏读），再查 ReadSet，
// 最后从 server 读取并将版本记录到 ReadSet 用于后续冲突检测。
func (h *TxHandle) Get(key string) (value string, version Tversion, err Err) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// 先查 WriteSet（脏读）
	if wk, ok := h.writeSet[key]; ok {
		return wk.Value, wk.Version, OK
	}

	// 再查 ReadSet
	if rk, ok := h.readSet[key]; ok {
		// Keep the first-read version as the optimistic validation baseline.
		return rk.Value, rk.ExpectedVersion, OK
	}

	// 从 server 读取
	val, ver, _, e := h.coordinator.clerkGet(key)
	if e != OK {
		return "", 0, e
	}

	h.readSet[key] = ReadKey{Key: key, Value: val, ExpectedVersion: ver}
	return val, ver, OK
}

// Put 在事务内缓存一个写操作。
func (h *TxHandle) Put(key string, value string, expectedVersion Tversion) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.writeSet[key] = WriteKey{Key: key, Value: value, Version: expectedVersion}
	gid := h.coordinator.router.Resolve(key)
	if gid >= 0 {
		h.groups[gid] = true
	}
}

// Delete 在事务内缓存一个删除操作。
// 先读取 key 的当前版本（优先从 readSet 取，避免额外网络往返），
// 用于 Commit 阶段的 CAS 校验——否则硬编码 Version: 0 会对已存在的 key
// 导致版本冲突、对不存在的 key 意外创建空条目。
func (h *TxHandle) Delete(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	version := Tversion(0)
	if rk, ok := h.readSet[key]; ok {
		version = rk.ExpectedVersion
	} else {
		_, ver, _, e := h.coordinator.clerkGet(key)
		if e == OK {
			version = ver
			h.readSet[key] = ReadKey{Key: key, ExpectedVersion: ver}
		}
	}

	h.writeSet[key] = WriteKey{Key: key, Value: "", Version: version, IsDelete: true}
	gid := h.coordinator.router.Resolve(key)
	if gid >= 0 {
		h.groups[gid] = true
	}
}

// Commit 执行完整的 2PC 协议：
//
//	Phase 1: 并行 PrepareTx 到所有涉及的 Group
//	Phase 2: 如果全部 PREPARED → 并行 CommitTx（幂等，最多重试 5 次）
//	         任何 Prepare 失败 → 并行 AbortTx 到已 Prepare 的 Group
func (h *TxHandle) Commit() Err {
	h.mu.Lock()
	writeKeysByGroup := h.groupWriteKeys()
	readKeysByGroup := h.groupReadKeys()
	// 检查是否有 key 无法映射到任何 Group（静默丢失会导致数据不一致）
	totalGroupedWrites := 0
	for _, wks := range writeKeysByGroup {
		totalGroupedWrites += len(wks)
	}
	if totalGroupedWrites < len(h.writeSet) {
		h.mu.Unlock()
		return ErrWrongGroup
	}
	allGroups := make([]int, 0, len(h.groups)+len(readKeysByGroup))
	seenGroups := make(map[int]bool)
	for gid := range h.groups {
		seenGroups[gid] = true
		allGroups = append(allGroups, gid)
	}
	for gid := range readKeysByGroup {
		if !seenGroups[gid] {
			allGroups = append(allGroups, gid)
		}
	}
	sort.Ints(allGroups)
	h.mu.Unlock()

	if len(allGroups) == 0 {
		return OK // 空事务（仅有读操作）
	}
	coordinatorGroup := allGroups[0]

	// Phase 1: 并行 Prepare
	type prepareResult struct {
		gid int
		err error
		res *pb.PrepareTxResponse
	}
	prepareCh := make(chan prepareResult, len(allGroups))
	for _, gid := range allGroups {
		go func(groupID int) {
			req := &pb.PrepareTxRequest{
				TxId:      h.txID,
				ReadKeys:  readKeysToProto(readKeysByGroup[groupID]),
				WriteKeys: writeKeysToProto(writeKeysByGroup[groupID]),
				TimeoutMs: h.timeoutMs, CoordinatorGroupId: int32(coordinatorGroup), ParticipantGroupIds: int32Slice(allGroups),
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			resp, err := h.coordinator.router.PrepareTxToGroup(ctx, groupID, req)
			prepareCh <- prepareResult{gid: groupID, err: err, res: resp}
		}(gid)
	}

	prepared := make(map[int]bool)
	var firstErr Err
	for i := 0; i < len(allGroups); i++ {
		result := <-prepareCh
		if result.err != nil {
			firstErr = ErrTxConflict
			continue
		}
		if result.res.GetError() != string(OK) {
			firstErr = mapPBErr(result.res.GetError())
			continue
		}
		prepared[result.gid] = true
	}

	// Any terminal action must follow a durable coordinator decision.
	if len(prepared) < len(allGroups) {
		if h.persistDecision(coordinatorGroup, allGroups, TxStatusAborted, nil) != OK {
			return ErrTxTimeout
		}
		h.parallelAbort(prepared)
		if firstErr != "" {
			return firstErr
		}
		return ErrTxConflict
	}

	// ===== Commit Point =====
	if h.persistDecision(coordinatorGroup, allGroups, TxStatusCommitted, writeKeysByGroup) != OK {
		return ErrTxTimeout
	}

	// 事务结果确定为 COMMITTED
	// Phase 2 不再决定返回结果，启动后台重试
	go h.retryCommitInBackground(allGroups, writeKeysByGroup)

	return OK
}

// parallelAbort 向所有给定 Group 发送 AbortTx（尽力而为）。
func (h *TxHandle) parallelAbort(groups map[int]bool) {
	var wg sync.WaitGroup
	for gid := range groups {
		wg.Add(1)
		go func(groupID int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
			defer cancel()
			_, _ = h.coordinator.router.AbortTxToGroup(ctx, groupID, &pb.AbortTxRequest{TxId: h.txID})
		}(gid)
	}
	wg.Wait()
}

// parallelCommit 向所有给定 Group 发送 CommitTx，带幂等重试。
func (h *TxHandle) parallelCommit(groups []int, writeKeysByGroup map[int][]WriteKey) Err {
	var wg sync.WaitGroup
	errCh := make(chan Err, len(groups))

	for _, gid := range groups {
		wks := writeKeysByGroup[gid]
		wg.Add(1)
		go func(groupID int, writeKeys []WriteKey) {
			defer wg.Done()
			req := &pb.CommitTxRequest{
				TxId:      h.txID,
				WriteKeys: writeKeysToProto(writeKeys),
			}
			for attempt := 0; attempt < txCommitMaxRetries; attempt++ {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				resp, err := h.coordinator.router.CommitTxToGroup(ctx, groupID, req)
				cancel()
				if err == nil && resp != nil && resp.GetError() == string(OK) {
					return
				}
				if resp != nil && resp.GetError() == string(ErrTxConflict) {
					// 已被 Abort — 无法恢复
					errCh <- ErrTxConflict
					return
				}
				time.Sleep(backoffWithJitter(attempt, retryBackoffBase, retryBackoffMax))
			}
			errCh <- ErrTxTimeout
		}(gid, wks)
	}
	wg.Wait()
	close(errCh)

	for e := range errCh {
		if e != OK {
			return e
		}
	}
	return OK
}

// retryCommitInBackground 后台持续重试 Phase 2，直到所有 Participant 完成
// 前提：Commit Point 已过，决策已持久化为 COMMITTED
func (h *TxHandle) retryCommitInBackground(groups []int, writeKeysByGroup map[int][]WriteKey) {
	attempt := 0

	for {
		err := h.parallelCommit(groups, writeKeysByGroup)

		if err == OK {
			log.Printf("[Coordinator] 事务 %s Phase 2 完成", h.txID)
			return
		}

		if err == ErrTxConflict {
			// 不应该发生（决策已是 COMMITTED）
			// 可能是 Participant 协议错误或临时故障
			log.Printf("[Coordinator] 事务 %s Phase 2 遇到冲突: %v，继续重试", h.txID, err)
		}

		// ErrTxTimeout: 部分 Participant 未响应，继续重试
		backoff := backoffWithJitter(attempt, retryBackoffBase, retryBackoffMax)
		log.Printf("[Coordinator] 事务 %s Phase 2 部分失败，%v 后重试", h.txID, backoff)

		time.Sleep(backoff)
		attempt++
	}
}

func (h *TxHandle) persistDecision(gid int, groups []int, decision TxStatus, writeKeysByGroup map[int][]WriteKey) Err {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ids := int32Slice(groups)

	// 收集所有 writeKeys
	var allWriteKeys []WriteKey
	for _, wks := range writeKeysByGroup {
		allWriteKeys = append(allWriteKeys, wks...)
	}

	if decision == TxStatusCommitted {
		resp, err := h.coordinator.router.CommitTxToGroup(ctx, gid, &pb.CommitTxRequest{
			TxId:                h.txID,
			DecisionOnly:        true,
			ParticipantGroupIds: ids,
			WriteKeys:           writeKeysToProto(allWriteKeys),
		})
		if err == nil && resp != nil && resp.GetError() == string(OK) {
			return OK
		}
	} else {
		resp, err := h.coordinator.router.AbortTxToGroup(ctx, gid, &pb.AbortTxRequest{TxId: h.txID, DecisionOnly: true, ParticipantGroupIds: ids})
		if err == nil && resp != nil && resp.GetError() == string(OK) {
			return OK
		}
	}
	return ErrTxTimeout
}

// RecoverTransaction resolves a prepared participant using the coordinator group's durable decision.
func (tc *TxCoordinator) RecoverTransaction(txID string, participantGroupID int) Err {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	participant, err := tc.router.ResolveTxStatusToGroup(ctx, participantGroupID, &pb.ResolveTxStatusRequest{TxId: txID})
	cancel()
	if err != nil || participant == nil {
		return ErrTxTimeout
	}
	if participant.GetStatus() == "COMMITTED" || participant.GetStatus() == "ABORTED" {
		groups := intsFromInt32(participant.GetParticipantGroupIds())
		if len(groups) == 0 {
			groups = []int{participantGroupID}
		}
		return recoverBroadcast(tc.router, txID, groups, participant.GetStatus() == "COMMITTED")
	}
	if participant.GetStatus() != "PREPARED" {
		return ErrTxTimeout
	}
	coordinator := int(participant.GetCoordinatorGroupId())
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	decision, err := tc.router.ResolveTxStatusToGroup(ctx, coordinator, &pb.ResolveTxStatusRequest{TxId: txID})
	cancel()
	if err != nil || decision == nil {
		return ErrTxTimeout
	}
	groups := intsFromInt32(decision.GetParticipantGroupIds())
	if len(groups) == 0 {
		groups = intsFromInt32(participant.GetParticipantGroupIds())
	}
	if decision.GetStatus() == "COMMITTED" {
		return recoverBroadcast(tc.router, txID, groups, true)
	}
	if decision.GetStatus() == "ABORTED" {
		return recoverBroadcast(tc.router, txID, groups, false)
	}
	return ErrTxTimeout
}

func recoverBroadcast(router *sharding.ShardRouter, txID string, groups []int, commit bool) Err {
	for _, gid := range groups {
		var lastErr error
		for attempt := 0; attempt < txCommitMaxRetries; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			if commit {
				resp, err := router.CommitTxToGroup(ctx, gid, &pb.CommitTxRequest{TxId: txID})
				if err == nil && resp != nil && resp.GetError() == string(OK) {
					cancel()
					lastErr = nil
					break
				}
				if err == nil && resp != nil {
					lastErr = fmt.Errorf("%s", resp.GetError())
				} else {
					lastErr = err
				}
			} else {
				resp, err := router.AbortTxToGroup(ctx, gid, &pb.AbortTxRequest{TxId: txID})
				if err == nil && resp != nil && resp.GetError() == string(OK) {
					cancel()
					lastErr = nil
					break
				}
				if err == nil && resp != nil {
					lastErr = fmt.Errorf("%s", resp.GetError())
				} else {
					lastErr = err
				}
			}
			cancel()
			if lastErr != nil && strings.Contains(lastErr.Error(), string(ErrTxConflict)) {
				break
			}
			if attempt+1 < txCommitMaxRetries {
				time.Sleep(backoffWithJitter(attempt, retryBackoffBase, retryBackoffMax))
			}
		}
		if lastErr != nil {
			return ErrTxTimeout
		}
	}
	return OK
}

// Rollback durably records ABORT before notifying prepared participants.
func (h *TxHandle) Rollback() {
	h.mu.Lock()
	groupSet := make(map[int]bool, len(h.groups)+len(h.readSet))
	for gid := range h.groups {
		groupSet[gid] = true
	}
	for _, rk := range h.readSet {
		if gid := h.coordinator.router.Resolve(rk.Key); gid >= 0 {
			groupSet[gid] = true
		}
	}
	groups := make([]int, 0, len(groupSet))
	prepared := make(map[int]bool, len(groupSet))
	for gid := range groupSet {
		groups = append(groups, gid)
		prepared[gid] = true
	}
	h.mu.Unlock()
	if len(groups) == 0 {
		return
	}
	sort.Ints(groups)
	if h.persistDecision(groups[0], groups, TxStatusAborted, nil) == OK {
		h.parallelAbort(prepared)
	}
}

// groupWriteKeys 将 WriteSet 按 Group ID 分组。
func (h *TxHandle) groupWriteKeys() map[int][]WriteKey {
	result := make(map[int][]WriteKey)
	for _, wk := range h.writeSet {
		gid := h.coordinator.router.Resolve(wk.Key)
		if gid >= 0 {
			result[gid] = append(result[gid], wk)
		}
	}
	return result
}

// groupReadKeys 将 ReadSet 按 Group ID 分组。
func (h *TxHandle) groupReadKeys() map[int][]ReadKey {
	result := make(map[int][]ReadKey)
	for _, rk := range h.readSet {
		gid := h.coordinator.router.Resolve(rk.Key)
		if gid >= 0 {
			result[gid] = append(result[gid], rk)
		}
	}
	return result
}

// readKeysToProto 将内部 ReadKey 转换为 proto 格式。
func readKeysToProto(keys []ReadKey) []*pb.ReadKey {
	if keys == nil {
		return nil
	}
	pbKeys := make([]*pb.ReadKey, 0, len(keys))
	for _, k := range keys {
		pbKeys = append(pbKeys, &pb.ReadKey{
			Key:             k.Key,
			ExpectedVersion: int64(k.ExpectedVersion),
		})
	}
	return pbKeys
}

// writeKeysToProto 将内部 WriteKey 转换为 proto 格式。
func writeKeysToProto(keys []WriteKey) []*pb.WriteKey {
	if keys == nil {
		return nil
	}
	pbKeys := make([]*pb.WriteKey, 0, len(keys))
	for _, k := range keys {
		pbKeys = append(pbKeys, &pb.WriteKey{
			Key:      k.Key,
			Value:    k.Value,
			Version:  int64(k.Version),
			IsDelete: k.IsDelete,
		})
	}
	return pbKeys
}

// clerkGet 通过 router 执行简单 Get（供 TxHandle.Get 使用）。
func (tc *TxCoordinator) clerkGet(key string) (string, Tversion, int64, Err) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	resp, err := tc.router.GetRoute(ctx, key)
	if err != nil || resp == nil {
		return "", 0, 0, ErrWrongLeader
	}
	errCode := mapPBErr(resp.GetError())
	if errCode != OK {
		return "", 0, 0, errCode
	}
	return resp.GetValue(), Tversion(resp.GetVersion()), resp.GetExpires(), OK
}

func int32Slice(in []int) []int32 {
	out := make([]int32, len(in))
	for i, v := range in {
		out[i] = int32(v)
	}
	return out
}

// retryCommitForRecoveredTx 为恢复的事务重试 Phase 2
// 与 TxHandle.retryCommitInBackground 类似，但不依赖 TxHandle
func (tc *TxCoordinator) retryCommitForRecoveredTx(
	txID string,
	groups []int,
	writeKeysByGroup map[int][]WriteKey,
) {
	attempt := 0

	for {
		// 构造临时 TxHandle 用于调用 parallelCommit
		h := &TxHandle{
			txID:        txID,
			coordinator: tc,
		}

		err := h.parallelCommit(groups, writeKeysByGroup)

		if err == OK {
			log.Printf("[Coordinator] 恢复事务 %s Phase 2 完成", txID)
			return
		}

		if err == ErrTxConflict {
			log.Printf("[Coordinator] 恢复事务 %s Phase 2 遇到冲突: %v，继续重试", txID, err)
		}

		// ErrTxTimeout: 部分 Participant 未响应，继续重试
		backoff := backoffWithJitter(attempt, retryBackoffBase, retryBackoffMax)
		log.Printf("[Coordinator] 恢复事务 %s Phase 2 部分失败，%v 后重试", txID, backoff)

		time.Sleep(backoff)
		attempt++
	}
}
