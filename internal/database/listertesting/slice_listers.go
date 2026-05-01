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
	"strings"

	"github.com/Azure/ARO-HCP/internal/api/kubeapplier"
	"github.com/Azure/ARO-HCP/internal/database"
	"github.com/Azure/ARO-HCP/internal/database/listers"
)

// SliceApplyDesireLister implements listers.ApplyDesireLister backed by a slice.
// Tests can populate Desires directly and the lister scans on every call.
type SliceApplyDesireLister struct {
	Desires []*kubeapplier.ApplyDesire
}

var _ listers.ApplyDesireLister = &SliceApplyDesireLister{}

func (l *SliceApplyDesireLister) List(ctx context.Context) ([]*kubeapplier.ApplyDesire, error) {
	return l.Desires, nil
}

func (l *SliceApplyDesireLister) GetForCluster(
	ctx context.Context, subscriptionID, resourceGroupName, clusterName, name string,
) (*kubeapplier.ApplyDesire, error) {
	want := kubeapplier.ToClusterScopedApplyDesireResourceIDString(subscriptionID, resourceGroupName, clusterName, name)
	for _, d := range l.Desires {
		id := resourceIDOf(d)
		if id != nil && strings.EqualFold(id.String(), want) {
			return d, nil
		}
	}
	return nil, database.NewNotFoundError()
}

func (l *SliceApplyDesireLister) GetForNodePool(
	ctx context.Context, subscriptionID, resourceGroupName, clusterName, nodePoolName, name string,
) (*kubeapplier.ApplyDesire, error) {
	want := kubeapplier.ToNodePoolScopedApplyDesireResourceIDString(
		subscriptionID, resourceGroupName, clusterName, nodePoolName, name,
	)
	for _, d := range l.Desires {
		id := resourceIDOf(d)
		if id != nil && strings.EqualFold(id.String(), want) {
			return d, nil
		}
	}
	return nil, database.NewNotFoundError()
}

func (l *SliceApplyDesireLister) ListForManagementCluster(
	ctx context.Context, managementCluster string,
) ([]*kubeapplier.ApplyDesire, error) {
	var out []*kubeapplier.ApplyDesire
	for _, d := range l.Desires {
		if strings.EqualFold(d.GetManagementCluster(), managementCluster) {
			out = append(out, d)
		}
	}
	return out, nil
}

func (l *SliceApplyDesireLister) ListForCluster(
	ctx context.Context, subscriptionID, resourceGroupName, clusterName string,
) ([]*kubeapplier.ApplyDesire, error) {
	var out []*kubeapplier.ApplyDesire
	for _, d := range l.Desires {
		if underCluster(resourceIDOf(d), subscriptionID, resourceGroupName, clusterName) {
			out = append(out, d)
		}
	}
	return out, nil
}

func (l *SliceApplyDesireLister) ListForNodePool(
	ctx context.Context, subscriptionID, resourceGroupName, clusterName, nodePoolName string,
) ([]*kubeapplier.ApplyDesire, error) {
	var out []*kubeapplier.ApplyDesire
	for _, d := range l.Desires {
		if underNodePool(resourceIDOf(d), subscriptionID, resourceGroupName, clusterName, nodePoolName) {
			out = append(out, d)
		}
	}
	return out, nil
}

// SliceDeleteDesireLister implements listers.DeleteDesireLister backed by a slice.
type SliceDeleteDesireLister struct {
	Desires []*kubeapplier.DeleteDesire
}

var _ listers.DeleteDesireLister = &SliceDeleteDesireLister{}

func (l *SliceDeleteDesireLister) List(ctx context.Context) ([]*kubeapplier.DeleteDesire, error) {
	return l.Desires, nil
}

func (l *SliceDeleteDesireLister) GetForCluster(
	ctx context.Context, subscriptionID, resourceGroupName, clusterName, name string,
) (*kubeapplier.DeleteDesire, error) {
	want := kubeapplier.ToClusterScopedDeleteDesireResourceIDString(subscriptionID, resourceGroupName, clusterName, name)
	for _, d := range l.Desires {
		id := resourceIDOf(d)
		if id != nil && strings.EqualFold(id.String(), want) {
			return d, nil
		}
	}
	return nil, database.NewNotFoundError()
}

func (l *SliceDeleteDesireLister) GetForNodePool(
	ctx context.Context, subscriptionID, resourceGroupName, clusterName, nodePoolName, name string,
) (*kubeapplier.DeleteDesire, error) {
	want := kubeapplier.ToNodePoolScopedDeleteDesireResourceIDString(
		subscriptionID, resourceGroupName, clusterName, nodePoolName, name,
	)
	for _, d := range l.Desires {
		id := resourceIDOf(d)
		if id != nil && strings.EqualFold(id.String(), want) {
			return d, nil
		}
	}
	return nil, database.NewNotFoundError()
}

func (l *SliceDeleteDesireLister) ListForManagementCluster(
	ctx context.Context, managementCluster string,
) ([]*kubeapplier.DeleteDesire, error) {
	var out []*kubeapplier.DeleteDesire
	for _, d := range l.Desires {
		if strings.EqualFold(d.GetManagementCluster(), managementCluster) {
			out = append(out, d)
		}
	}
	return out, nil
}

func (l *SliceDeleteDesireLister) ListForCluster(
	ctx context.Context, subscriptionID, resourceGroupName, clusterName string,
) ([]*kubeapplier.DeleteDesire, error) {
	var out []*kubeapplier.DeleteDesire
	for _, d := range l.Desires {
		if underCluster(resourceIDOf(d), subscriptionID, resourceGroupName, clusterName) {
			out = append(out, d)
		}
	}
	return out, nil
}

func (l *SliceDeleteDesireLister) ListForNodePool(
	ctx context.Context, subscriptionID, resourceGroupName, clusterName, nodePoolName string,
) ([]*kubeapplier.DeleteDesire, error) {
	var out []*kubeapplier.DeleteDesire
	for _, d := range l.Desires {
		if underNodePool(resourceIDOf(d), subscriptionID, resourceGroupName, clusterName, nodePoolName) {
			out = append(out, d)
		}
	}
	return out, nil
}

// SliceReadDesireLister implements listers.ReadDesireLister backed by a slice.
type SliceReadDesireLister struct {
	Desires []*kubeapplier.ReadDesire
}

var _ listers.ReadDesireLister = &SliceReadDesireLister{}

func (l *SliceReadDesireLister) List(ctx context.Context) ([]*kubeapplier.ReadDesire, error) {
	return l.Desires, nil
}

func (l *SliceReadDesireLister) GetForCluster(
	ctx context.Context, subscriptionID, resourceGroupName, clusterName, name string,
) (*kubeapplier.ReadDesire, error) {
	want := kubeapplier.ToClusterScopedReadDesireResourceIDString(subscriptionID, resourceGroupName, clusterName, name)
	for _, d := range l.Desires {
		id := resourceIDOf(d)
		if id != nil && strings.EqualFold(id.String(), want) {
			return d, nil
		}
	}
	return nil, database.NewNotFoundError()
}

func (l *SliceReadDesireLister) GetForNodePool(
	ctx context.Context, subscriptionID, resourceGroupName, clusterName, nodePoolName, name string,
) (*kubeapplier.ReadDesire, error) {
	want := kubeapplier.ToNodePoolScopedReadDesireResourceIDString(
		subscriptionID, resourceGroupName, clusterName, nodePoolName, name,
	)
	for _, d := range l.Desires {
		id := resourceIDOf(d)
		if id != nil && strings.EqualFold(id.String(), want) {
			return d, nil
		}
	}
	return nil, database.NewNotFoundError()
}

func (l *SliceReadDesireLister) ListForManagementCluster(
	ctx context.Context, managementCluster string,
) ([]*kubeapplier.ReadDesire, error) {
	var out []*kubeapplier.ReadDesire
	for _, d := range l.Desires {
		if strings.EqualFold(d.GetManagementCluster(), managementCluster) {
			out = append(out, d)
		}
	}
	return out, nil
}

func (l *SliceReadDesireLister) ListForCluster(
	ctx context.Context, subscriptionID, resourceGroupName, clusterName string,
) ([]*kubeapplier.ReadDesire, error) {
	var out []*kubeapplier.ReadDesire
	for _, d := range l.Desires {
		if underCluster(resourceIDOf(d), subscriptionID, resourceGroupName, clusterName) {
			out = append(out, d)
		}
	}
	return out, nil
}

func (l *SliceReadDesireLister) ListForNodePool(
	ctx context.Context, subscriptionID, resourceGroupName, clusterName, nodePoolName string,
) ([]*kubeapplier.ReadDesire, error) {
	var out []*kubeapplier.ReadDesire
	for _, d := range l.Desires {
		if underNodePool(resourceIDOf(d), subscriptionID, resourceGroupName, clusterName, nodePoolName) {
			out = append(out, d)
		}
	}
	return out, nil
}
