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

package database

import (
	"context"
	"fmt"
	"path"
	"strings"

	azcorearm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"

	"github.com/Azure/ARO-HCP/internal/api/fleet"
)

// fleetResourceCRUD is the fleet-container counterpart to nestedCosmosResourceCRUD.
// It stores a partition key at construction time so that all operations are scoped
// to a single fleet partition (stamp identifier).
type fleetResourceCRUD[InternalAPIType, CosmosAPIType any] struct {
	containerClient  *azcosmos.ContainerClient
	parentResourceID *azcorearm.ResourceID
	resourceType     azcorearm.ResourceType
	partitionKey     string
}

func newFleetResourceCRUD[InternalAPIType, CosmosAPIType any](
	containerClient *azcosmos.ContainerClient,
	stampIdentifier string,
	resourceType azcorearm.ResourceType,
) *fleetResourceCRUD[InternalAPIType, CosmosAPIType] {
	parentResourceID, err := fleet.ToFleetResourceID(stampIdentifier)
	if err != nil {
		panic(fmt.Sprintf("invalid stamp identifier %q: %v", stampIdentifier, err))
	}
	return &fleetResourceCRUD[InternalAPIType, CosmosAPIType]{
		containerClient:  containerClient,
		parentResourceID: parentResourceID,
		resourceType:     resourceType,
		partitionKey:     strings.ToLower(stampIdentifier),
	}
}

func (d *fleetResourceCRUD[InternalAPIType, CosmosAPIType]) makeResourceIDPath(
	resourceName string,
) (*azcorearm.ResourceID, error) {
	parts := []string{d.parentResourceID.String()}
	parts = append(parts, d.resourceType.Types[len(d.resourceType.Types)-1])
	if len(resourceName) > 0 {
		parts = append(parts, resourceName)
	}
	return azcorearm.ParseResourceID(strings.ToLower(path.Join(parts...)))
}

func (d *fleetResourceCRUD[InternalAPIType, CosmosAPIType]) GetByID(
	ctx context.Context, cosmosID string,
) (*InternalAPIType, error) {
	if strings.ToLower(cosmosID) != cosmosID {
		return nil, fmt.Errorf("cosmosID must be lowercase, not: %q", cosmosID)
	}
	return getByItemID[InternalAPIType, CosmosAPIType](ctx, d.containerClient, d.partitionKey, cosmosID)
}

func (d *fleetResourceCRUD[InternalAPIType, CosmosAPIType]) Get(
	ctx context.Context, resourceName string,
) (*InternalAPIType, error) {
	resourceID, err := d.makeResourceIDPath(resourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to make ResourceID path for '%s': %w", resourceName, err)
	}
	return get[InternalAPIType, CosmosAPIType](ctx, d.containerClient, d.partitionKey, resourceID)
}

func (d *fleetResourceCRUD[InternalAPIType, CosmosAPIType]) List(
	ctx context.Context, options *DBClientListResourceDocsOptions,
) (DBClientIterator[InternalAPIType], error) {
	prefix, err := d.makeResourceIDPath("")
	if err != nil {
		return nil, fmt.Errorf("failed to make ResourceID prefix: %w", err)
	}
	return list[InternalAPIType, CosmosAPIType](
		ctx, d.containerClient, d.partitionKey, &d.resourceType, prefix, options, false,
	)
}

func (d *fleetResourceCRUD[InternalAPIType, CosmosAPIType]) Create(
	ctx context.Context, newObj *InternalAPIType, options *azcosmos.ItemOptions,
) (*InternalAPIType, error) {
	return createFleetItem[InternalAPIType, CosmosAPIType](
		ctx, d.containerClient, d.partitionKey, newObj, options,
	)
}

func (d *fleetResourceCRUD[InternalAPIType, CosmosAPIType]) Replace(
	ctx context.Context, newObj *InternalAPIType, options *azcosmos.ItemOptions,
) (*InternalAPIType, error) {
	return replaceFleetItem[InternalAPIType, CosmosAPIType](
		ctx, d.containerClient, d.partitionKey, newObj, options,
	)
}

func (d *fleetResourceCRUD[InternalAPIType, CosmosAPIType]) Delete(
	ctx context.Context, resourceName string,
) error {
	resourceID, err := d.makeResourceIDPath(resourceName)
	if err != nil {
		return fmt.Errorf("failed to make ResourceID path for '%s': %w", resourceName, err)
	}
	return deleteResource(ctx, d.containerClient, d.partitionKey, resourceID)
}

func (d *fleetResourceCRUD[InternalAPIType, CosmosAPIType]) AddCreateToTransaction(
	ctx context.Context,
	transaction DBTransaction,
	newObj *InternalAPIType,
	opts *azcosmos.TransactionalBatchItemOptions,
) (string, error) {
	return addFleetCreateToTransaction[InternalAPIType, CosmosAPIType](ctx, transaction, newObj, opts)
}

func (d *fleetResourceCRUD[InternalAPIType, CosmosAPIType]) AddReplaceToTransaction(
	ctx context.Context,
	transaction DBTransaction,
	newObj *InternalAPIType,
	opts *azcosmos.TransactionalBatchItemOptions,
) (string, error) {
	return addFleetReplaceToTransaction[InternalAPIType, CosmosAPIType](ctx, transaction, newObj, opts)
}

var _ ResourceCRUD[fleet.ManagementCluster] = &fleetResourceCRUD[fleet.ManagementCluster, GenericDocument[fleet.ManagementCluster]]{}
