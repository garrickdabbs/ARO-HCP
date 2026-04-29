package kubeapplier

import (
	"github.com/Azure/ARO-HCP/internal/api"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type ApplyDesire struct {
	// CosmosMetadata ResourceID is nested under the cluster or nodepool so that association and cleanup work as expected
	// it will be the ApplyDesire type
	api.CosmosMetadata `json:"cosmosMetadata"`

	Spec ApplyDesireSpec `json:"spec"`

	Status ApplyDesireStatus `json:"status"`
}

type ApplyDesireSpec struct {
	// ManagementCluster specifies the identifier for the management cluster responsible for handling the desired state application.
	// TODO this may end up changing to be a resourceID
	ManagementCluster string `json:"managementCluster"`

	// KubeContent must be singular, not a list.
	// It will be sent via server-side-apply with a force.
	KubeContent runtime.RawExtension `json:"kubeContent"`
}

type ApplyDesireStatus struct {
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}
