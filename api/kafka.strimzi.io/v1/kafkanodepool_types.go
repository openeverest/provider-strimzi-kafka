package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// KafkaNodePool is the Strimzi KafkaNodePool custom resource.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type KafkaNodePool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KafkaNodePoolSpec   `json:"spec,omitempty"`
	Status KafkaNodePoolStatus `json:"status,omitempty"`
}

// KafkaNodePoolSpec is the desired state of a node pool.
// replicas, roles, and storage are required by the Strimzi KafkaNodePool CRD.
type KafkaNodePoolSpec struct {
	Replicas  int32                        `json:"replicas"`
	Roles     []string                     `json:"roles"` // enum: controller, broker
	Storage   KafkaNodePoolStorage         `json:"storage"`
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
}

// KafkaNodePoolStorage configures storage for nodes in the pool.
type KafkaNodePoolStorage struct {
	// Type is ephemeral, persistent-claim, or jbod.
	Type        string                    `json:"type"`
	Size        string                    `json:"size,omitempty"`
	Class       string                    `json:"class,omitempty"`
	DeleteClaim *bool                     `json:"deleteClaim,omitempty"`
	Volumes     []KafkaNodePoolStorageVol `json:"volumes,omitempty"`
}

// KafkaNodePoolStorageVol is a volume in a jbod storage configuration.
// type is required on each volume by the Strimzi CRD.
type KafkaNodePoolStorageVol struct {
	ID            *int32 `json:"id,omitempty"`
	Type          string `json:"type"` // ephemeral | persistent-claim
	Size          string `json:"size,omitempty"`
	Class         string `json:"class,omitempty"`
	DeleteClaim   *bool  `json:"deleteClaim,omitempty"`
	KraftMetadata string `json:"kraftMetadata,omitempty"` // enum: shared
}

// KafkaNodePoolStatus is the observed state of a node pool.
type KafkaNodePoolStatus struct {
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	Replicas           int32              `json:"replicas,omitempty"`
	LabelSelector      string             `json:"labelSelector,omitempty"`
	NodeIDs            []int32            `json:"nodeIds,omitempty"`
	ClusterID          string             `json:"clusterId,omitempty"`
	Roles              []string           `json:"roles,omitempty"`
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
}

// KafkaNodePoolList contains a list of KafkaNodePool.
//
// +kubebuilder:object:root=true
type KafkaNodePoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KafkaNodePool `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KafkaNodePool{}, &KafkaNodePoolList{})
}
