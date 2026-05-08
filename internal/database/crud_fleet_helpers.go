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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"

	"github.com/Azure/ARO-HCP/internal/api/arm"
	"github.com/Azure/ARO-HCP/internal/api/fleet"
)

// serializeFleetItem mirrors serializeItem but validates the partition key
// against the fleet resource's stamp identifier instead of the resourceID's
// subscriptionID.
func serializeFleetItem[InternalAPIType, CosmosAPIType any](
	newObj *InternalAPIType,
) (*arm.CosmosMetadata, []byte, error) {
	cosmosPersistable, ok := any(newObj).(arm.CosmosPersistable)
	if !ok {
		return nil, nil, fmt.Errorf("type %T does not implement CosmosPersistable interface", newObj)
	}
	stampAccessor, ok := any(newObj).(fleet.FleetPartitionKeyAccessor)
	if !ok {
		return nil, nil, fmt.Errorf("type %T does not implement FleetPartitionKeyAccessor", newObj)
	}
	cosmosData := cosmosPersistable.GetCosmosData()
	cosmosUID := cosmosData.GetCosmosUID()
	if len(cosmosUID) == 0 {
		return nil, nil, fmt.Errorf("no cosmos id found in object")
	}
	if !strings.EqualFold(cosmosUID, strings.ToLower(cosmosUID)) {
		return nil, nil, fmt.Errorf("invalid cosmos id found in object")
	}
	if len(stampAccessor.GetStampIdentifier()) == 0 {
		return nil, nil, fmt.Errorf("fleet object %T has empty stamp identifier (ResourceID.Name)", newObj)
	}

	cosmosObj, err := InternalToCosmos[InternalAPIType, CosmosAPIType](newObj)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert internal object to Cosmos object: %w", err)
	}
	data, err := json.Marshal(cosmosObj)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal Cosmos DB item for '%s': %w", cosmosData.ResourceID, err)
	}

	return cosmosData, data, nil
}

// fleetPartitionKey returns the lowercased stamp identifier from a fleet resource.
func fleetPartitionKey[InternalAPIType any](newObj *InternalAPIType) (string, error) {
	stampAccessor, ok := any(newObj).(fleet.FleetPartitionKeyAccessor)
	if !ok {
		return "", fmt.Errorf("type %T does not implement FleetPartitionKeyAccessor", newObj)
	}
	pk := strings.ToLower(stampAccessor.GetStampIdentifier())
	if len(pk) == 0 {
		return "", fmt.Errorf("fleet object %T has empty stamp identifier (ResourceID.Name)", newObj)
	}
	return pk, nil
}

func createFleetItem[InternalAPIType, CosmosAPIType any](
	ctx context.Context,
	containerClient *azcosmos.ContainerClient,
	partitionKeyString string,
	newObj *InternalAPIType,
	opts *azcosmos.ItemOptions,
) (*InternalAPIType, error) {
	if strings.ToLower(partitionKeyString) != partitionKeyString {
		return nil, fmt.Errorf("partitionKeyString must be lowercase, not: %q", partitionKeyString)
	}
	cosmosMetadata, data, err := serializeFleetItem[InternalAPIType, CosmosAPIType](newObj)
	if err != nil {
		return nil, err
	}
	objPK, err := fleetPartitionKey(newObj)
	if err != nil {
		return nil, err
	}
	if partitionKeyString != objPK {
		return nil, fmt.Errorf(
			"item stamp identifier does not match partition key: %q vs %q",
			objPK, partitionKeyString,
		)
	}

	if opts == nil {
		opts = &azcosmos.ItemOptions{}
	}
	opts.EnableContentResponseOnWrite = true

	responseItem, err := containerClient.CreateItem(ctx, azcosmos.NewPartitionKeyString(partitionKeyString), data, opts)
	if err != nil {
		return nil, err
	}

	return responseItemToInternalObj[InternalAPIType, CosmosAPIType](ctx, cosmosMetadata.GetCosmosUID(), responseItem)
}

func replaceFleetItem[InternalAPIType, CosmosAPIType any](
	ctx context.Context,
	containerClient *azcosmos.ContainerClient,
	partitionKeyString string,
	newObj *InternalAPIType,
	opts *azcosmos.ItemOptions,
) (*InternalAPIType, error) {
	if strings.ToLower(partitionKeyString) != partitionKeyString {
		return nil, fmt.Errorf("partitionKeyString must be lowercase, not: %q", partitionKeyString)
	}
	cosmosMetadata, data, err := serializeFleetItem[InternalAPIType, CosmosAPIType](newObj)
	if err != nil {
		return nil, err
	}
	objPK, err := fleetPartitionKey(newObj)
	if err != nil {
		return nil, err
	}
	if partitionKeyString != objPK {
		return nil, fmt.Errorf(
			"item stamp identifier does not match partition key: %q vs %q",
			objPK, partitionKeyString,
		)
	}

	if opts == nil {
		opts = &azcosmos.ItemOptions{}
	}
	if len(cosmosMetadata.CosmosETag) > 0 {
		opts.IfMatchEtag = &cosmosMetadata.CosmosETag
	}
	opts.EnableContentResponseOnWrite = true

	responseItem, err := containerClient.ReplaceItem(
		ctx, azcosmos.NewPartitionKeyString(partitionKeyString), cosmosMetadata.GetCosmosUID(), data, opts,
	)
	if err != nil {
		return nil, err
	}

	return responseItemToInternalObj[InternalAPIType, CosmosAPIType](ctx, cosmosMetadata.GetCosmosUID(), responseItem)
}

func addFleetCreateToTransaction[InternalAPIType, CosmosAPIType any](
	ctx context.Context,
	transaction DBTransaction,
	newObj *InternalAPIType,
	opts *azcosmos.TransactionalBatchItemOptions,
) (string, error) {
	partitionKeyString := transaction.GetPartitionKey()
	if strings.ToLower(partitionKeyString) != partitionKeyString {
		return "", fmt.Errorf("partitionKeyString must be lowercase, not: %q", partitionKeyString)
	}
	cosmosMetadata, data, err := serializeFleetItem[InternalAPIType, CosmosAPIType](newObj)
	if err != nil {
		return "", err
	}
	objPK, err := fleetPartitionKey(newObj)
	if err != nil {
		return "", err
	}
	if partitionKeyString != objPK {
		return "", fmt.Errorf(
			"item stamp identifier does not match partition key: %q vs %q",
			objPK, partitionKeyString,
		)
	}
	transactionDetails := CosmosDBTransactionStepDetails{
		ActionType: "Create",
		GoType:     fmt.Sprintf("%T", newObj),
		CosmosID:   cosmosMetadata.GetCosmosUID(),
		ResourceID: cosmosMetadata.ResourceID.String(),
	}

	transaction.AddStep(
		transactionDetails,
		func(b *azcosmos.TransactionalBatch) (string, error) {
			b.CreateItem(data, opts)
			return cosmosMetadata.GetCosmosUID(), nil
		},
	)

	return cosmosMetadata.GetCosmosUID(), nil
}

func addFleetReplaceToTransaction[InternalAPIType, CosmosAPIType any](
	ctx context.Context,
	transaction DBTransaction,
	newObj *InternalAPIType,
	opts *azcosmos.TransactionalBatchItemOptions,
) (string, error) {
	partitionKeyString := transaction.GetPartitionKey()
	if strings.ToLower(partitionKeyString) != partitionKeyString {
		return "", fmt.Errorf("partitionKeyString must be lowercase, not: %q", partitionKeyString)
	}
	cosmosMetadata, data, err := serializeFleetItem[InternalAPIType, CosmosAPIType](newObj)
	if err != nil {
		return "", err
	}
	objPK, err := fleetPartitionKey(newObj)
	if err != nil {
		return "", err
	}
	if partitionKeyString != objPK {
		return "", fmt.Errorf(
			"item stamp identifier does not match partition key: %q vs %q",
			objPK, partitionKeyString,
		)
	}
	transactionDetails := CosmosDBTransactionStepDetails{
		ActionType: "Replace",
		GoType:     fmt.Sprintf("%T", newObj),
		CosmosID:   cosmosMetadata.GetCosmosUID(),
		ResourceID: cosmosMetadata.ResourceID.String(),
		Etag:       cosmosMetadata.CosmosETag,
	}

	if opts == nil {
		opts = &azcosmos.TransactionalBatchItemOptions{}
	}
	if len(cosmosMetadata.CosmosETag) > 0 {
		opts.IfMatchETag = &cosmosMetadata.CosmosETag
	}

	transaction.AddStep(
		transactionDetails,
		func(b *azcosmos.TransactionalBatch) (string, error) {
			b.ReplaceItem(cosmosMetadata.GetCosmosUID(), data, opts)
			return cosmosMetadata.GetCosmosUID(), nil
		},
	)

	return cosmosMetadata.GetCosmosUID(), nil
}
