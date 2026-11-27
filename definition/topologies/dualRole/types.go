// Package dualrole contains parameter types for the dualRole topology.
//
// +k8s:openapi-gen=true
package dualrole

// DualRoleTopologyParameters is reserved for topology-level knobs.
// Currently empty — dual-role clusters are fully described by the engine
// component (replicas, resources, storage) and Kafka version.
type DualRoleTopologyParameters struct{}
