// Package definition contains shared types used across the provider definition.
//
// +k8s:openapi-gen=true
package definition

// TopologyType is a known topology name declared under definition/topologies/.
type TopologyType string

const (
	// TopologyTypeDualRole is a KRaft cluster with one KafkaNodePool whose
	// nodes run both controller and broker roles.
	// Matches Strimzi examples/kafka/kafka-with-dual-role-nodes.yaml and
	// examples/kafka/kafka-single-node.yaml (1.3.1).
	TopologyTypeDualRole TopologyType = "dualRole"
)
