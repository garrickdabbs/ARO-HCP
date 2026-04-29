package kubeapplier

import (
	"github.com/Azure/ARO-HCP/internal/api"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type ReadDesire struct {
	// CosmosMetadata ResourceID is nested under the cluster or nodepool so that association and cleanup work as expected
	// it will be the ReadDesire type
	api.CosmosMetadata `json:"cosmosMetadata"`

	Spec ReadDesireSpec `json:"spec"`

	Status ReadDesireStatus `json:"status"`
}

type ReadDesireSpec struct {
	// ManagementCluster specifies the identifier for the management cluster responsible for handling the desired state application.
	// TODO this may end up changing to be a resourceID
	ManagementCluster string `json:"managementCluster"`

	// TargetItem is a group, resource, namespace, name that will be read from the Management Cluster.
	// It will periodically refresh its view. There is no guarantee of frequency or inidication of age.
	TargetItem ResourceReference `json:"targetItem,omitempty"`
}

type ResourceReference struct {
	Group     string `json:"group"`
	Resource  string `json:"resource"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`
}

type ReadDesireStatus struct {
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	KubeContent runtime.RawExtension `json:"kubeContent"`
}
