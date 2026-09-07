package sharding

import "testing"

func TestExplicitOwnershipSurvivesRouterReload(t *testing.T) {
	groups := map[int][]string{1: {"a"}, 2: {"b"}}
	owners := []int{1, 1, 2, 2}
	st := NewShardTopologyWithOwnership(4, groups, owners, 7)
	if st == nil || st.GetEpoch() != 7 {
		t.Fatal("explicit topology was not created")
	}
	for i, want := range owners {
		got, ok := st.GroupForShard(i)
		if !ok || got != want {
			t.Fatalf("shard %d owner=%d/%v want=%d", i, got, ok, want)
		}
	}
}

func TestPlanAddGroupDoesNotMutateCurrentTopology(t *testing.T) {
	groups := map[int][]string{1: {"a"}, 2: {"b"}}
	st := NewShardTopologyWithOwnership(8, groups, []int{1, 1, 1, 1, 2, 2, 2, 2}, 3)
	owners, moved := st.PlanAddGroup(3, []string{"c"})
	if len(owners) != 8 || len(moved) != 2 {
		t.Fatalf("owners=%v moved=%v", owners, moved)
	}
	if len(st.GroupIDs()) != 2 || st.GetEpoch() != 3 {
		t.Fatal("planning mutated current topology")
	}
}
