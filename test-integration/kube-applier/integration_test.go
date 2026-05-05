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

// Package kube_applier_integration runs the kube-applier controllers in-process
// against a real kube-apiserver provided by sigs.k8s.io/controller-runtime's
// envtest (etcd + kube-apiserver binaries; no Docker required) and a mock
// Cosmos KubeApplierClient. Each test is described by an artifact directory
// under ./artifacts/. See ./framework for step types and conventions.
//
// The tests are skipped if KUBEBUILDER_ASSETS is unset; see the package
// README for setup instructions.
package kube_applier_integration

import (
	"embed"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/Azure/ARO-HCP/test-integration/kube-applier/framework"
)

//go:embed artifacts
var artifacts embed.FS

var (
	testEnv *envtest.Environment
	cfg     *rest.Config
)

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		// envtest binaries (etcd + kube-apiserver) are not installed in this
		// environment. Skip the whole suite cleanly so a `go test ./...` run
		// across the workspace doesn't fail.
		os.Exit(0)
	}

	testEnv = &envtest.Environment{}
	var err error
	cfg, err = testEnv.Start()
	if err != nil {
		panic(err)
	}

	code := m.Run()

	_ = testEnv.Stop()
	os.Exit(code)
}

func TestKubeApplierIntegration(t *testing.T) {
	cases, err := framework.LoadTestCases(artifacts, "artifacts")
	require.NoError(t, err)
	require.NotEmpty(t, cases, "no artifact directories found under ./artifacts")

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) { tc.RunCase(t, cfg) })
	}
}
