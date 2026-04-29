package kubeapplier

import (
	"github.com/Azure/ARO-HCP/internal/api"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type DeleteDesire struct {
	// CosmosMetadata ResourceID is nested under the cluster or nodepool so that association and cleanup work as expected
	// it will be the DeleteDesire type
	api.CosmosMetadata `json:"cosmosMetadata"`

	Spec DeleteDesireSpec `json:"spec"`

	Status DeleteDesireStatus `json:"status"`
}

type DeleteDesireSpec struct {
	// ManagementCluster specifies the identifier for the management cluster responsible for handling the desired state application.
	// TODO this may end up changing to be a resourceID
	ManagementCluster string `json:"managementCluster"`

	// TargetItem is a group, resource, namespace, name that will be deleted from the Management Cluster
	TargetItem ResourceReference `json:"targetItem,omitempty"`
}

type DeleteDesireStatus struct {
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}
