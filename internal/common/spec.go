// Package common defines shared constants used across the provider.
package common

const (
	// ProviderName is this provider's identity: the name of the Provider CR it
	// ships and the value Instances put in spec.providerRef.name. It must match
	// `name` in definition/provider.yaml — the runtime uses it to fetch the
	// Provider CR, so a mismatch means nothing ever reconciles.
	ProviderName = "strimzi-kafka"

	// ComponentEngine is the dual-role Kafka node pool component.
	ComponentEngine = "engine"

	// ComponentTypeKafka is the engine component type in versions.yaml.
	ComponentTypeKafka = "kafka"

	// DualRoleNodePoolName is the KafkaNodePool metadata.name used for the
	// dualRole topology. Matches Strimzi examples/kafka/kafka-with-dual-role-nodes.yaml.
	DualRoleNodePoolName = "dual-role"

	// StrimziClusterLabel is required on KafkaNodePool to bind it to a Kafka CR.
	// Source: Strimzi docs / examples (metadata.labels.strimzi.io/cluster).
	StrimziClusterLabel = "strimzi.io/cluster"

	// ListenerPlain is the internal plaintext listener name from Strimzi examples.
	ListenerPlain = "plain"
	// ListenerTLS is the internal TLS listener name from Strimzi examples.
	ListenerTLS = "tls"
)
