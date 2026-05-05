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

//go:generate $MOCKGEN -typed -source=crud_management_cluster.go -destination=mock_crud_management_cluster.go -package database ManagementClusterCRUD

import (
	"context"
	"fmt"
	"strings"

	azcorearm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"

	"github.com/Azure/ARO-HCP/internal/api/fleet"
	"github.com/Azure/ARO-HCP/internal/validation"
)

// ManagementClusterCRUD provides CRUD operations for ManagementCluster documents in CosmosDB.
// Create validates the object before writing. Replace validates the update
// against the old object before writing.
type ManagementClusterCRUD interface {
	GetByID(ctx context.Context, cosmosID string) (*fleet.ManagementCluster, error)
	Get(ctx context.Context, resourceID string) (*fleet.ManagementCluster, error)
	List(ctx context.Context, opts *DBClientListResourceDocsOptions) (DBClientIterator[fleet.ManagementCluster], error)
	Create(ctx context.Context, newObj *fleet.ManagementCluster, options *azcosmos.ItemOptions) (*fleet.ManagementCluster, error)
	Replace(ctx context.Context, newObj, oldObj *fleet.ManagementCluster, options *azcosmos.ItemOptions) (*fleet.ManagementCluster, error)
	Delete(ctx context.Context, resourceID string) error
}

type managementClusterCRUD struct {
	containerClient *azcosmos.ContainerClient
	resourceType    azcorearm.ResourceType
}

var _ ManagementClusterCRUD = &managementClusterCRUD{}

func NewManagementClusterCRUD(containerClient *azcosmos.ContainerClient) ManagementClusterCRUD {
	return &managementClusterCRUD{
		containerClient: containerClient,
		resourceType:    fleet.ManagementClusterResourceType,
	}
}

func (d *managementClusterCRUD) GetByID(
	ctx context.Context, cosmosID string,
) (*fleet.ManagementCluster, error) {
	if strings.ToLower(cosmosID) != cosmosID {
		return nil, fmt.Errorf("cosmosID must be lowercase, not: %q", cosmosID)
	}
	return getByItemID[fleet.ManagementCluster, GenericDocument[fleet.ManagementCluster]](ctx, d.containerClient, "", cosmosID)
}

func (d *managementClusterCRUD) Get(
	ctx context.Context, stampIdentifier string,
) (*fleet.ManagementCluster, error) {
	partitionKey := strings.ToLower(stampIdentifier)
	resourceID, err := fleet.ToManagementClusterResourceID(stampIdentifier)
	if err != nil {
		return nil, fmt.Errorf("failed to make ResourceID for stamp '%s': %w", stampIdentifier, err)
	}
	return get[fleet.ManagementCluster, GenericDocument[fleet.ManagementCluster]](ctx, d.containerClient, partitionKey, resourceID)
}

func (d *managementClusterCRUD) List(
	ctx context.Context, options *DBClientListResourceDocsOptions,
) (DBClientIterator[fleet.ManagementCluster], error) {
	return list[fleet.ManagementCluster, GenericDocument[fleet.ManagementCluster]](
		ctx, d.containerClient, "", &d.resourceType, nil, options, false,
	)
}

func (d *managementClusterCRUD) Create(
	ctx context.Context, newObj *fleet.ManagementCluster, options *azcosmos.ItemOptions,
) (*fleet.ManagementCluster, error) {
	if errs := validation.ValidateManagementClusterCreate(ctx, newObj); errs.ToAggregate() != nil {
		return nil, fmt.Errorf("management cluster %s validation failed: %w", newObj.ResourceID, errs.ToAggregate())
	}
	pk, err := fleetPartitionKey(newObj)
	if err != nil {
		return nil, err
	}
	return createFleet[fleet.ManagementCluster, GenericDocument[fleet.ManagementCluster]](
		ctx, d.containerClient, pk, newObj, options,
	)
}

func (d *managementClusterCRUD) Replace(
	ctx context.Context, newObj, oldObj *fleet.ManagementCluster, options *azcosmos.ItemOptions,
) (*fleet.ManagementCluster, error) {
	if errs := validation.ValidateManagementClusterUpdate(ctx, newObj, oldObj); errs.ToAggregate() != nil {
		return nil, fmt.Errorf("management cluster %s validation failed: %w", newObj.ResourceID, errs.ToAggregate())
	}
	pk, err := fleetPartitionKey(newObj)
	if err != nil {
		return nil, err
	}
	return replaceFleet[fleet.ManagementCluster, GenericDocument[fleet.ManagementCluster]](
		ctx, d.containerClient, pk, newObj, options,
	)
}

func (d *managementClusterCRUD) Delete(
	ctx context.Context, stampIdentifier string,
) error {
	partitionKey := strings.ToLower(stampIdentifier)
	resourceID, err := fleet.ToManagementClusterResourceID(stampIdentifier)
	if err != nil {
		return fmt.Errorf("failed to make ResourceID for stamp '%s': %w", stampIdentifier, err)
	}
	return deleteResource(ctx, d.containerClient, partitionKey, resourceID)
}
