// Copyright 2026 Microsoft Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package listertesting

import (
	"context"
	"testing"

	azcorearm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"

	"github.com/Azure/ARO-HCP/internal/api"
	"github.com/Azure/ARO-HCP/internal/api/kubeapplier"
	"github.com/Azure/ARO-HCP/internal/database"
)

const (
	testSub      = "00000000-0000-0000-0000-000000000001"
	testRG       = "rg"
	testCluster  = "c"
	testNodePool = "np"
	testMgmtA    = "mgmt-a"
	testMgmtB    = "mgmt-b"
)

func mustParseID(t *testing.T, s string) *azcorearm.ResourceID {
	t.Helper()
	id, err := azcorearm.ParseResourceID(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return id
}

func newApplyDesire(t *testing.T, idStr, mgmt string) *kubeapplier.ApplyDesire {
	t.Helper()
	return &kubeapplier.ApplyDesire{
		CosmosMetadata: api.CosmosMetadata{ResourceID: mustParseID(t, idStr)},
		Spec:           kubeapplier.ApplyDesireSpec{ManagementCluster: mgmt},
	}
}

// fixtureDesires returns four ApplyDesires:
//   - clusterA: mgmt-a, cluster-scoped, name=a1
//   - clusterA-np: mgmt-a, nodepool-scoped, name=a2
//   - clusterB: mgmt-b, cluster-scoped (different cluster), name=b1
//   - clusterB-other-rg: mgmt-b, cluster-scoped, different rg
func fixtureDesires(t *testing.T) []*kubeapplier.ApplyDesire {
	t.Helper()
	return []*kubeapplier.ApplyDesire{
		newApplyDesire(t,
			kubeapplier.ToClusterScopedApplyDesireResourceIDString(testSub, testRG, testCluster, "a1"),
			testMgmtA),
		newApplyDesire(t,
			kubeapplier.ToNodePoolScopedApplyDesireResourceIDString(testSub, testRG, testCluster, testNodePool, "a2"),
			testMgmtA),
		newApplyDesire(t,
			kubeapplier.ToClusterScopedApplyDesireResourceIDString(testSub, testRG, "other-cluster", "b1"),
			testMgmtB),
		newApplyDesire(t,
			kubeapplier.ToClusterScopedApplyDesireResourceIDString(testSub, "other-rg", testCluster, "b2"),
			testMgmtB),
	}
}

func TestSliceApplyDesireLister_List(t *testing.T) {
	ctx := context.Background()
	l := &SliceApplyDesireLister{Desires: fixtureDesires(t)}
	got, err := l.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 4 {
		t.Errorf("List() len = %d, want 4", len(got))
	}
}

func TestSliceApplyDesireLister_GetForCluster(t *testing.T) {
	ctx := context.Background()
	l := &SliceApplyDesireLister{Desires: fixtureDesires(t)}

	// Existing cluster-scoped desire returns the right item.
	got, err := l.GetForCluster(ctx, testSub, testRG, testCluster, "a1")
	if err != nil {
		t.Fatalf("GetForCluster a1: %v", err)
	}
	if got == nil || got.GetManagementCluster() != testMgmtA {
		t.Errorf("GetForCluster a1: unexpected result %+v", got)
	}

	// A name that exists only as a nodepool-scoped desire is NotFound at the cluster scope.
	if _, err := l.GetForCluster(ctx, testSub, testRG, testCluster, "a2"); !database.IsNotFoundError(err) {
		t.Errorf("GetForCluster a2 (nodepool-only): want NotFound, got %v", err)
	}

	// Wrong subscription — NotFound.
	if _, err := l.GetForCluster(ctx, "different-sub", testRG, testCluster, "a1"); !database.IsNotFoundError(err) {
		t.Errorf("GetForCluster wrong sub: want NotFound, got %v", err)
	}
}

func TestSliceApplyDesireLister_GetForNodePool(t *testing.T) {
	ctx := context.Background()
	l := &SliceApplyDesireLister{Desires: fixtureDesires(t)}

	got, err := l.GetForNodePool(ctx, testSub, testRG, testCluster, testNodePool, "a2")
	if err != nil {
		t.Fatalf("GetForNodePool a2: %v", err)
	}
	if got == nil {
		t.Fatal("GetForNodePool a2: nil")
	}

	// A name that only exists as cluster-scoped is NotFound at nodepool scope.
	if _, err := l.GetForNodePool(ctx, testSub, testRG, testCluster, testNodePool, "a1"); !database.IsNotFoundError(err) {
		t.Errorf("GetForNodePool a1 (cluster-only): want NotFound, got %v", err)
	}
}

func TestSliceApplyDesireLister_ListForManagementCluster(t *testing.T) {
	ctx := context.Background()
	l := &SliceApplyDesireLister{Desires: fixtureDesires(t)}

	gotA, err := l.ListForManagementCluster(ctx, testMgmtA)
	if err != nil {
		t.Fatalf("ListForManagementCluster mgmt-a: %v", err)
	}
	if len(gotA) != 2 {
		t.Errorf("ListForManagementCluster mgmt-a: len = %d, want 2 (cluster + nodepool)", len(gotA))
	}

	gotB, err := l.ListForManagementCluster(ctx, testMgmtB)
	if err != nil {
		t.Fatalf("ListForManagementCluster mgmt-b: %v", err)
	}
	if len(gotB) != 2 {
		t.Errorf("ListForManagementCluster mgmt-b: len = %d, want 2", len(gotB))
	}

	// Case-insensitive.
	gotUpperA, err := l.ListForManagementCluster(ctx, "MGMT-A")
	if err != nil {
		t.Fatalf("ListForManagementCluster MGMT-A: %v", err)
	}
	if len(gotUpperA) != 2 {
		t.Errorf("ListForManagementCluster MGMT-A: len = %d, want 2 (case-insensitive)", len(gotUpperA))
	}
}

func TestSliceApplyDesireLister_ListForCluster_IncludesNodePoolScoped(t *testing.T) {
	ctx := context.Background()
	l := &SliceApplyDesireLister{Desires: fixtureDesires(t)}

	got, err := l.ListForCluster(ctx, testSub, testRG, testCluster)
	if err != nil {
		t.Fatalf("ListForCluster: %v", err)
	}
	// Should pick up both a1 (cluster-scoped) AND a2 (nodepool-scoped under this cluster).
	if len(got) != 2 {
		t.Errorf("ListForCluster len = %d, want 2 (cluster + nodepool under cluster)", len(got))
	}

	// Different cluster name yields different (smaller) set.
	gotOther, err := l.ListForCluster(ctx, testSub, testRG, "other-cluster")
	if err != nil {
		t.Fatalf("ListForCluster other-cluster: %v", err)
	}
	if len(gotOther) != 1 {
		t.Errorf("ListForCluster other-cluster len = %d, want 1", len(gotOther))
	}
}

func TestSliceApplyDesireLister_ListForNodePool_OnlyNodePoolScoped(t *testing.T) {
	ctx := context.Background()
	l := &SliceApplyDesireLister{Desires: fixtureDesires(t)}

	got, err := l.ListForNodePool(ctx, testSub, testRG, testCluster, testNodePool)
	if err != nil {
		t.Fatalf("ListForNodePool: %v", err)
	}
	// Only the nodepool-scoped a2 should match — NOT the cluster-scoped a1.
	if len(got) != 1 {
		t.Errorf("ListForNodePool len = %d, want 1 (nodepool-scoped only)", len(got))
	}
}
