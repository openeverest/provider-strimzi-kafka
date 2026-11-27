package v1

import (
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Kafka is the Strimzi Kafka custom resource.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type Kafka struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KafkaSpec   `json:"spec,omitempty"`
	Status KafkaStatus `json:"status,omitempty"`
}

// KafkaSpec is the desired state of a Kafka cluster.
type KafkaSpec struct {
	Kafka          KafkaClusterSpec           `json:"kafka"`
	EntityOperator *EntityOperatorSpec        `json:"entityOperator,omitempty"`
}

// KafkaClusterSpec configures the Kafka brokers (KRaft).
// listeners is required by the Strimzi Kafka CRD.
type KafkaClusterSpec struct {
	Version         string                 `json:"version,omitempty"`
	MetadataVersion string                 `json:"metadataVersion,omitempty"`
	Listeners       []GenericKafkaListener `json:"listeners"`
	// Config is broker configuration. Kept as raw JSON so numeric values from
	// the CR (e.g. replication.factor: 3) round-trip correctly.
	// CRD: object with x-kubernetes-preserve-unknown-fields.
	Config *apiextensionsv1.JSON `json:"config,omitempty"`
}

// GenericKafkaListener configures a Kafka listener.
type GenericKafkaListener struct {
	Name string `json:"name"`
	Port int32  `json:"port"`
	// Type is typically internal, route, loadbalancer, nodeport, or ingress.
	Type string `json:"type"`
	TLS  bool   `json:"tls"`
}

// EntityOperatorSpec configures the topic and user operators.
type EntityOperatorSpec struct {
	TopicOperator *EntityTopicOperatorSpec `json:"topicOperator,omitempty"`
	UserOperator  *EntityUserOperatorSpec  `json:"userOperator,omitempty"`
}

// EntityTopicOperatorSpec configures the Topic Operator.
// An empty object enables defaults.
type EntityTopicOperatorSpec struct{}

// EntityUserOperatorSpec configures the User Operator.
// An empty object enables defaults.
type EntityUserOperatorSpec struct{}

// KafkaStatus is the observed state of a Kafka cluster.
type KafkaStatus struct {
	Conditions      []metav1.Condition      `json:"conditions,omitempty"`
	Listeners       []ListenerStatus        `json:"listeners,omitempty"`
	ClusterID       string                  `json:"clusterId,omitempty"`
	KafkaVersion    string                  `json:"kafkaVersion,omitempty"`
	KafkaNodePools  []KafkaNodePoolRefStatus `json:"kafkaNodePools,omitempty"`
}

// ListenerStatus reports addresses for a configured listener.
type ListenerStatus struct {
	Name             string            `json:"name,omitempty"`
	BootstrapServers string            `json:"bootstrapServers,omitempty"`
	Addresses        []ListenerAddress `json:"addresses,omitempty"`
	Certificates     []string          `json:"certificates,omitempty"`
}

// ListenerAddress is a host/port pair for a listener.
type ListenerAddress struct {
	Host string `json:"host,omitempty"`
	Port int32  `json:"port,omitempty"`
}

// KafkaNodePoolRefStatus references a node pool belonging to the cluster.
type KafkaNodePoolRefStatus struct {
	Name string `json:"name,omitempty"`
}

// KafkaList contains a list of Kafka.
//
// +kubebuilder:object:root=true
type KafkaList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Kafka `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Kafka{}, &KafkaList{})
}
