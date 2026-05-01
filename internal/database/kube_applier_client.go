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

	"k8s.io/utils/ptr"

	"github.com/Azure/ARO-HCP/internal/api"
	"github.com/Azure/ARO-HCP/internal/api/kubeapplier"
	"github.com/Azure/ARO-HCP/internal/utils"
)

// KubeApplierClient is the database surface used by the kube-applier binary.
// It is intentionally narrower than DBClient because the kube-applier pod's
// Cosmos credentials are scoped to a single container; reusing DBClient would
// expose methods (HCPClusters, Operations, &hellip;) that the pod cannot
// actually serve at runtime.
type KubeApplierClient interface {
	// KubeApplier returns CRUD accessors scoped to a single management-cluster
	// partition. The kube-applier binary calls this with its own management
	// cluster name; the backend (creator of *Desires) calls it with the
	// cluster it intends to write *Desires for.
	KubeApplier(managementCluster string) KubeApplierCRUD

	// GlobalListers returns cross-partition listers for the *Desire types.
	// Only callers with container-wide credentials (i.e. the backend) should
	// use this.
	GlobalListers() KubeApplierGlobalListers
}

// KubeApplierCRUD scopes ResourceCRUD accessors to a single management-cluster
// partition. It is constructed from KubeApplierClient.KubeApplier(managementCluster).
type KubeApplierCRUD interface {
	ApplyDesires(parent ResourceParent) (ResourceCRUD[kubeapplier.ApplyDesire], error)
	DeleteDesires(parent ResourceParent) (ResourceCRUD[kubeapplier.DeleteDesire], error)
	ReadDesires(parent ResourceParent) (ResourceCRUD[kubeapplier.ReadDesire], error)
}

// KubeApplierGlobalListers provides cross-partition listers for the three
// *Desire types in the kube-applier container.
type KubeApplierGlobalListers interface {
	ApplyDesires() GlobalLister[kubeapplier.ApplyDesire]
	DeleteDesires() GlobalLister[kubeapplier.DeleteDesire]
	ReadDesires() GlobalLister[kubeapplier.ReadDesire]
}

// ResourceParent identifies what a *Desire is nested under in the resource ID
// hierarchy. NodePoolName is optional: leave it empty for cluster-scoped desires.
type ResourceParent struct {
	SubscriptionID    string
	ResourceGroupName string
	ClusterName       string
	NodePoolName      string
}

// IsNodePoolScoped reports whether the parent identifies a node pool (not just a cluster).
func (p ResourceParent) IsNodePoolScoped() bool {
	return len(p.NodePoolName) != 0
}

// resourceID returns the parent resource ID used as the prefix for nested *Desires.
func (p ResourceParent) resourceID() (*azcorearm.ResourceID, error) {
	if len(p.SubscriptionID) == 0 {
		return nil, fmt.Errorf("subscriptionID is required")
	}
	if len(p.ResourceGroupName) == 0 {
		return nil, fmt.Errorf("resourceGroupName is required")
	}
	if len(p.ClusterName) == 0 {
		return nil, fmt.Errorf("clusterName is required")
	}
	parts := []string{
		"/subscriptions", strings.ToLower(p.SubscriptionID),
		"resourceGroups", p.ResourceGroupName,
		"providers", api.ClusterResourceType.String(), p.ClusterName,
	}
	if p.IsNodePoolScoped() {
		parts = append(parts, api.NodePoolResourceTypeName, p.NodePoolName)
	}
	return azcorearm.ParseResourceID(strings.ToLower(path.Join(parts...)))
}

// cosmosKubeApplierClient implements KubeApplierClient against a Cosmos container.
type cosmosKubeApplierClient struct {
	container *azcosmos.ContainerClient
}

var _ KubeApplierClient = &cosmosKubeApplierClient{}

// NewKubeApplierClient instantiates a KubeApplierClient from a Cosmos
// DatabaseClient. It opens *only* the kube-applier container; the caller's
// credentials therefore need only that single grant.
func NewKubeApplierClient(database *azcosmos.DatabaseClient) (KubeApplierClient, error) {
	container, err := database.NewContainer(kubeApplierContainer)
	if err != nil {
		return nil, utils.TrackError(err)
	}
	return &cosmosKubeApplierClient{container: container}, nil
}

// NewKubeApplierClientFromContainer wraps an already-opened container client.
// Useful when the caller has constructed the container client itself.
func NewKubeApplierClientFromContainer(container *azcosmos.ContainerClient) KubeApplierClient {
	return &cosmosKubeApplierClient{container: container}
}

func (c *cosmosKubeApplierClient) KubeApplier(managementCluster string) KubeApplierCRUD {
	return &kubeApplierCRUD{
		containerClient:   c.container,
		managementCluster: managementCluster,
	}
}

func (c *cosmosKubeApplierClient) GlobalListers() KubeApplierGlobalListers {
	return &cosmosKubeApplierGlobalListers{container: c.container}
}

// kubeApplierCRUD implements KubeApplierCRUD against a Cosmos container.
type kubeApplierCRUD struct {
	containerClient   *azcosmos.ContainerClient
	managementCluster string
}

var _ KubeApplierCRUD = &kubeApplierCRUD{}

func (k *kubeApplierCRUD) ApplyDesires(parent ResourceParent) (ResourceCRUD[kubeapplier.ApplyDesire], error) {
	parentID, err := parent.resourceID()
	if err != nil {
		return nil, err
	}
	resourceType := kubeapplier.ClusterScopedApplyDesireResourceType
	if parent.IsNodePoolScoped() {
		resourceType = kubeapplier.NodePoolScopedApplyDesireResourceType
	}
	return newKubeApplierResourceCRUD[kubeapplier.ApplyDesire, GenericDocument[kubeapplier.ApplyDesire]](
		k.containerClient, k.managementCluster, parentID, resourceType,
	), nil
}

func (k *kubeApplierCRUD) DeleteDesires(parent ResourceParent) (ResourceCRUD[kubeapplier.DeleteDesire], error) {
	parentID, err := parent.resourceID()
	if err != nil {
		return nil, err
	}
	resourceType := kubeapplier.ClusterScopedDeleteDesireResourceType
	if parent.IsNodePoolScoped() {
		resourceType = kubeapplier.NodePoolScopedDeleteDesireResourceType
	}
	return newKubeApplierResourceCRUD[kubeapplier.DeleteDesire, GenericDocument[kubeapplier.DeleteDesire]](
		k.containerClient, k.managementCluster, parentID, resourceType,
	), nil
}

func (k *kubeApplierCRUD) ReadDesires(parent ResourceParent) (ResourceCRUD[kubeapplier.ReadDesire], error) {
	parentID, err := parent.resourceID()
	if err != nil {
		return nil, err
	}
	resourceType := kubeapplier.ClusterScopedReadDesireResourceType
	if parent.IsNodePoolScoped() {
		resourceType = kubeapplier.NodePoolScopedReadDesireResourceType
	}
	return newKubeApplierResourceCRUD[kubeapplier.ReadDesire, GenericDocument[kubeapplier.ReadDesire]](
		k.containerClient, k.managementCluster, parentID, resourceType,
	), nil
}

// cosmosKubeApplierGlobalListers implements KubeApplierGlobalListers against a Cosmos container.
type cosmosKubeApplierGlobalListers struct {
	container *azcosmos.ContainerClient
}

var _ KubeApplierGlobalListers = &cosmosKubeApplierGlobalListers{}

func (g *cosmosKubeApplierGlobalListers) ApplyDesires() GlobalLister[kubeapplier.ApplyDesire] {
	return &cosmosKubeApplierDesireGlobalLister[kubeapplier.ApplyDesire, GenericDocument[kubeapplier.ApplyDesire]]{
		containerClient: g.container,
		resourceTypes: []azcorearm.ResourceType{
			kubeapplier.ClusterScopedApplyDesireResourceType,
			kubeapplier.NodePoolScopedApplyDesireResourceType,
		},
	}
}

func (g *cosmosKubeApplierGlobalListers) DeleteDesires() GlobalLister[kubeapplier.DeleteDesire] {
	return &cosmosKubeApplierDesireGlobalLister[kubeapplier.DeleteDesire, GenericDocument[kubeapplier.DeleteDesire]]{
		containerClient: g.container,
		resourceTypes: []azcorearm.ResourceType{
			kubeapplier.ClusterScopedDeleteDesireResourceType,
			kubeapplier.NodePoolScopedDeleteDesireResourceType,
		},
	}
}

func (g *cosmosKubeApplierGlobalListers) ReadDesires() GlobalLister[kubeapplier.ReadDesire] {
	return &cosmosKubeApplierDesireGlobalLister[kubeapplier.ReadDesire, GenericDocument[kubeapplier.ReadDesire]]{
		containerClient: g.container,
		resourceTypes: []azcorearm.ResourceType{
			kubeapplier.ClusterScopedReadDesireResourceType,
			kubeapplier.NodePoolScopedReadDesireResourceType,
		},
	}
}

// cosmosKubeApplierDesireGlobalLister lists *Desire documents (one kind per
// instance) across every management-cluster partition of the kube-applier
// container, unioning the cluster-scoped and node-pool-scoped resource types
// in a single cross-partition query.
type cosmosKubeApplierDesireGlobalLister[InternalAPIType, CosmosAPIType any] struct {
	containerClient *azcosmos.ContainerClient
	resourceTypes   []azcorearm.ResourceType
}

func (l *cosmosKubeApplierDesireGlobalLister[InternalAPIType, CosmosAPIType]) List(
	ctx context.Context, options *DBClientListResourceDocsOptions,
) (DBClientIterator[InternalAPIType], error) {
	var resourceTypeConditions []string
	for _, rt := range l.resourceTypes {
		resourceTypeConditions = append(
			resourceTypeConditions,
			fmt.Sprintf("STRINGEQUALS(c.resourceType, %q, true)", rt.String()),
		)
	}
	query := fmt.Sprintf("SELECT * FROM c WHERE %s", strings.Join(resourceTypeConditions, " OR "))

	queryOptions := azcosmos.QueryOptions{PageSizeHint: -1}
	if options != nil {
		if options.PageSizeHint != nil {
			queryOptions.PageSizeHint = max(*options.PageSizeHint, -1)
		}
		queryOptions.ContinuationToken = options.ContinuationToken
	}

	pager := l.containerClient.NewQueryItemsPager(query, azcosmos.NewPartitionKey(), &queryOptions)

	if options != nil && ptr.Deref(options.PageSizeHint, -1) > 0 {
		return newQueryResourcesSinglePageIterator[InternalAPIType, CosmosAPIType](pager), nil
	}
	return newQueryResourcesIterator[InternalAPIType, CosmosAPIType](pager), nil
}
