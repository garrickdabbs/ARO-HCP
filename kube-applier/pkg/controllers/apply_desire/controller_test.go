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

package apply_desire

import (
	"context"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"

	azcorearm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"

	"github.com/Azure/ARO-HCP/internal/api"
	"github.com/Azure/ARO-HCP/internal/api/kubeapplier"
	"github.com/Azure/ARO-HCP/kube-applier/pkg/controllers/conditions"
)

func mustParseID(t *testing.T, s string) *azcorearm.ResourceID {
	t.Helper()
	id, err := azcorearm.ParseResourceID(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return id
}

// fakeDynamic returns a dynamic.Interface backed by an in-memory tracker that
// supports Apply (via Patch with ApplyPatchType under the covers).
func fakeDynamic(t *testing.T, gvrToListKind map[schema.GroupVersionResource]string) *fake.FakeDynamicClient {
	t.Helper()
	scheme := runtime.NewScheme()
	return fake.NewSimpleDynamicClientWithCustomListKinds(scheme, gvrToListKind)
}

func configMapTarget(name string) kubeapplier.ResourceReference {
	return kubeapplier.ResourceReference{
		Group: "", Version: "v1", Resource: "configmaps", Namespace: "default", Name: name,
	}
}

// newApplyDesire builds an ApplyDesire with a populated TargetItem and
// kubeContent JSON. Pass nil kubeContent to exercise the empty-kubeContent
// PreCheck. Pass a partial target to exercise the targetItem-validation PreChecks.
func newApplyDesire(t *testing.T, name string, target kubeapplier.ResourceReference, kubeContent []byte) *kubeapplier.ApplyDesire {
	t.Helper()
	return &kubeapplier.ApplyDesire{
		CosmosMetadata: api.CosmosMetadata{
			ResourceID: mustParseID(t, kubeapplier.ToClusterScopedApplyDesireResourceIDString(
				"00000000-0000-0000-0000-000000000001", "rg", "cluster", name,
			)),
		},
		Spec: kubeapplier.ApplyDesireSpec{
			ManagementCluster: "mgmt-1",
			TargetItem:        target,
			KubeContent:       runtime.RawExtension{Raw: kubeContent},
		},
	}
}

// TestApplyDesired_IssuesSSAPatch verifies the controller issues the expected
// SSA call (Apply patch type, Force=true, FieldManager=kube-applier, correct
// namespace+name) for a well-formed ApplyDesire.
//
// We assert on the action tracker rather than the resulting object: the fake
// dynamic client's Apply path strategic-merges via the Unstructured scheme,
// which doesn't have the typed metadata SMP needs, so the post-apply object
// is unreliable. End-to-end SSA semantics are covered by integration tests.
func TestApplyDesired_IssuesSSAPatch(t *testing.T) {
	ctx := context.Background()
	gvr := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	dyn := fakeDynamic(t, map[schema.GroupVersionResource]string{gvr: "ConfigMapList"})
	dyn.PrependReactor("patch", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"})
		obj.SetName(action.(clienttesting.PatchAction).GetName())
		obj.SetNamespace(action.GetNamespace())
		return true, obj, nil
	})

	c := &ApplyDesireController{dyn: dyn}
	desire := newApplyDesire(t, "ok", configMapTarget("hello"), []byte(`{
	  "apiVersion": "v1",
	  "kind": "ConfigMap",
	  "metadata": {"name":"hello", "namespace":"default"},
	  "data": {"k":"v"}
	}`))
	if err := c.applyDesired(ctx, desire); err != nil {
		t.Fatalf("applyDesired: %v", err)
	}

	actions := dyn.Actions()
	var patch clienttesting.PatchAction
	for _, a := range actions {
		if pa, ok := a.(clienttesting.PatchAction); ok {
			patch = pa
			break
		}
	}
	if patch == nil {
		t.Fatalf("no patch action recorded; actions=%v", actions)
	}
	if patch.GetPatchType() != types.ApplyPatchType {
		t.Errorf("patch type = %v, want ApplyPatchType", patch.GetPatchType())
	}
	if got := patch.GetName(); got != "hello" {
		t.Errorf("patch name = %q, want hello", got)
	}
	if got := patch.GetNamespace(); got != "default" {
		t.Errorf("patch namespace = %q, want default", got)
	}
}

// TestApplyDesired_PreCheckErrors covers every pre-flight failure that must
// classify as PreCheckError (and therefore land as Successful=False with
// reason PreCheckFailed in higher-level code).
func TestApplyDesired_PreCheckErrors(t *testing.T) {
	ctx := context.Background()
	dyn := fakeDynamic(t, map[schema.GroupVersionResource]string{
		{Group: "", Version: "v1", Resource: "configmaps"}: "ConfigMapList",
	})
	c := &ApplyDesireController{dyn: dyn}

	validKubeContent := []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"x","namespace":"default"}}`)

	cases := []struct {
		name        string
		target      kubeapplier.ResourceReference
		kubeContent []byte
		wantSubstr  string
	}{
		{
			name:        "missing version in targetItem",
			target:      kubeapplier.ResourceReference{Resource: "configmaps", Namespace: "default", Name: "x"},
			kubeContent: validKubeContent,
			wantSubstr:  "version, resource, and name",
		},
		{
			name:        "missing resource in targetItem",
			target:      kubeapplier.ResourceReference{Version: "v1", Namespace: "default", Name: "x"},
			kubeContent: validKubeContent,
			wantSubstr:  "version, resource, and name",
		},
		{
			name:        "empty kubeContent",
			target:      configMapTarget("x"),
			kubeContent: nil,
			wantSubstr:  "spec.kubeContent is empty",
		},
		{
			name:        "malformed kubeContent JSON",
			target:      configMapTarget("x"),
			kubeContent: []byte("not json"),
			wantSubstr:  "decode kubeContent",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := c.applyDesired(ctx, newApplyDesire(t, "x", tc.target, tc.kubeContent))
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantSubstr)
			}
			if _, ok := err.(*conditions.PreCheckError); !ok {
				t.Errorf("error %v is not a *PreCheckError; classification will be wrong", err)
			}
			if !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantSubstr)
			}
		})
	}
}
