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

package databasetesting

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	azcorearm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"

	"github.com/Azure/ARO-HCP/internal/api/fleet"
	"github.com/Azure/ARO-HCP/internal/database"
)

// MockFleetDBClient is the in-memory test double for database.FleetDBClient.
// It owns its own document store, separate from MockResourcesDBClient —
// production has the fleet container live in a different Cosmos container
// (and behind different credentials), and the mock mirrors that boundary.
type MockFleetDBClient struct {
	mu        sync.RWMutex
	documents map[string]json.RawMessage
}

var _ database.FleetDBClient = &MockFleetDBClient{}
var _ mockDocumentStore = &MockFleetDBClient{}

// NewMockFleetDBClient creates an empty MockFleetDBClient.
func NewMockFleetDBClient() *MockFleetDBClient {
	return &MockFleetDBClient{
		documents: make(map[string]json.RawMessage),
	}
}

// NewMockFleetDBClientWithResources creates a MockFleetDBClient and populates
// it with the given resources. Supported types:
//   - *fleet.ManagementCluster
func NewMockFleetDBClientWithResources(ctx context.Context, resources []any) (*MockFleetDBClient, error) {
	mock := NewMockFleetDBClient()
	for i, r := range resources {
		if err := mock.addResource(ctx, r); err != nil {
			return nil, fmt.Errorf("failed to add resource at index %d: %w", i, err)
		}
	}
	return mock, nil
}

func (m *MockFleetDBClient) addResource(ctx context.Context, resource any) error {
	switch r := resource.(type) {
	case *fleet.ManagementCluster:
		return m.addManagementCluster(ctx, r)
	default:
		return fmt.Errorf("unsupported resource type for MockFleetDBClient: %T", resource)
	}
}

func (m *MockFleetDBClient) addManagementCluster(ctx context.Context, mc *fleet.ManagementCluster) error {
	crud := newMockManagementClusterCRUD(m, nil)
	_, err := crud.Create(ctx, mc, nil)
	return err
}

// --- mockDocumentStore implementation ---

func (m *MockFleetDBClient) GetDocument(cosmosID string) (json.RawMessage, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.documents[strings.ToLower(cosmosID)]
	return data, ok
}

func (m *MockFleetDBClient) StoreDocument(cosmosID string, data json.RawMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.documents[strings.ToLower(cosmosID)] = data
}

func (m *MockFleetDBClient) DeleteDocument(cosmosID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.documents, strings.ToLower(cosmosID))
}

func (m *MockFleetDBClient) ListDocuments(resourceType *azcorearm.ResourceType, prefix string) []json.RawMessage {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var results []json.RawMessage
	for _, data := range m.documents {
		var td database.TypedDocument
		if err := json.Unmarshal(data, &td); err != nil {
			continue
		}
		if resourceType != nil && !strings.EqualFold(td.ResourceType, resourceType.String()) {
			continue
		}
		if len(prefix) != 0 && td.ResourceID != nil &&
			!strings.HasPrefix(strings.ToLower(td.ResourceID.String()), strings.ToLower(prefix)) {
			continue
		}
		results = append(results, data)
	}
	return results
}

func (m *MockFleetDBClient) GetAllDocuments() map[string]json.RawMessage {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]json.RawMessage, len(m.documents))
	for k, v := range m.documents {
		out[k] = v
	}
	return out
}

// --- FleetDBClient implementation ---

func (m *MockFleetDBClient) ManagementClusters() database.ManagementClusterCRUD {
	return newMockManagementClusterCRUD(m, nil)
}

func (m *MockFleetDBClient) GlobalListers() database.FleetGlobalListers {
	return &mockFleetGlobalListers{client: m}
}

// --- FleetGlobalListers ---

type mockFleetGlobalListers struct {
	client mockDocumentStore
}

var _ database.FleetGlobalListers = &mockFleetGlobalListers{}

func (g *mockFleetGlobalListers) ManagementClusters() database.GlobalLister[fleet.ManagementCluster] {
	return &mockTypedGlobalLister[fleet.ManagementCluster, database.GenericDocument[fleet.ManagementCluster]]{
		client:       g.client,
		resourceType: fleet.ManagementClusterResourceType,
	}
}
