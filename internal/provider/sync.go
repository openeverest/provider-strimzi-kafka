package provider

import (
	"encoding/json"
	"fmt"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	kafkav1 "github.com/openeverest/provider-strimzi-kafka/api/kafka.strimzi.io/v1"
	"github.com/openeverest/provider-strimzi-kafka/internal/common"
)

// metadataVersions maps Kafka.spec.kafka.version → metadataVersion for the
// versions we ship. Values come from Strimzi 1.3.1 kafka-versions.yaml
// (`metadata:` field), e.g. 4.3.2 → 4.3-IV0.
// https://github.com/strimzi/strimzi-kafka-operator/blob/1.3.1/kafka-versions.yaml
var metadataVersions = map[string]string{
	common.DefaultKafkaVersion: "4.3-IV0",
}

func metadataVersionFor(kafkaVersion string) (string, bool) {
	mv, ok := metadataVersions[kafkaVersion]
	return mv, ok
}

func resolveKafkaVersion(c *controller.Context, engine corev1alpha1.ComponentSpec) string {
	if engine.Version != "" {
		return engine.Version
	}
	if v := c.Instance().Spec.Version; v != "" {
		return v
	}
	return common.DefaultKafkaVersion
}

// replicationSettings mirrors Strimzi example configs:
//   - kafka-single-node.yaml (replicas 1): RF=1, min.isr=1
//   - kafka-with-dual-role-nodes.yaml (replicas 3): RF=3, min.isr=2
// Cap RF at 3 so larger odd sizes (5) keep the same defaults as the 3-node example.
func replicationSettings(replicas int32) (rf, minISR int32) {
	rf = replicas
	if rf > 3 {
		rf = 3
	}
	if rf < 1 {
		rf = 1
	}
	minISR = 1
	if rf > 1 {
		minISR = rf - 1
	}
	return rf, minISR
}

func buildBrokerConfig(replicas int32) (*apiextensionsv1.JSON, error) {
	rf, minISR := replicationSettings(replicas)
	raw, err := json.Marshal(map[string]int32{
		"offsets.topic.replication.factor":         rf,
		"transaction.state.log.replication.factor": rf,
		"transaction.state.log.min.isr":             minISR,
		"default.replication.factor":                rf,
		"min.insync.replicas":                        minISR,
	})
	if err != nil {
		return nil, err
	}
	return &apiextensionsv1.JSON{Raw: raw}, nil
}

func buildNodePool(c *controller.Context, engine corev1alpha1.ComponentSpec) *kafkav1.KafkaNodePool {
	meta := c.ObjectMeta(common.DualRoleNodePoolName(c.Name()))
	if meta.Labels == nil {
		meta.Labels = map[string]string{}
	}
	meta.Labels[common.StrimziClusterLabel] = c.Name()

	volID := int32(0)
	deleteClaim := true
	storage := kafkav1.KafkaNodePoolStorage{
		Type: "jbod",
		Volumes: []kafkav1.KafkaNodePoolStorageVol{{
			ID:            &volID,
			Type:          "persistent-claim",
			Size:          engine.Storage.Size.String(),
			DeleteClaim:   &deleteClaim,
			KraftMetadata: "shared",
		}},
	}
	if engine.Storage.StorageClass != nil && *engine.Storage.StorageClass != "" {
		storage.Volumes[0].Class = *engine.Storage.StorageClass
	}

	podLabels := c.PodLabels(common.ComponentEngine)
	pool := &kafkav1.KafkaNodePool{
		ObjectMeta: meta,
		Spec: kafkav1.KafkaNodePoolSpec{
			Replicas: *engine.Replicas,
			Roles:    []string{"controller", "broker"},
			Storage:  storage,
			Template: &kafkav1.KafkaNodePoolTemplate{
				Pod: &kafkav1.PodTemplate{
					Metadata: &kafkav1.ResourceMetadata{Labels: podLabels},
				},
			},
		},
	}
	if engine.Resources != nil {
		pool.Spec.Resources = engine.Resources.DeepCopy()
	}
	return pool
}

func buildKafka(c *controller.Context, engine corev1alpha1.ComponentSpec) (*kafkav1.Kafka, error) {
	version := resolveKafkaVersion(c, engine)
	metadataVersion, ok := metadataVersionFor(version)
	if !ok {
		return nil, fmt.Errorf("unsupported Kafka version %q", version)
	}
	config, err := buildBrokerConfig(*engine.Replicas)
	if err != nil {
		return nil, err
	}

	return &kafkav1.Kafka{
		ObjectMeta: c.ObjectMeta(c.Name()),
		Spec: kafkav1.KafkaSpec{
			Kafka: kafkav1.KafkaClusterSpec{
				Version:         version,
				MetadataVersion: metadataVersion,
				// Listeners match Strimzi dual-role / single-node examples.
				Listeners: []kafkav1.GenericKafkaListener{
					{Name: common.ListenerPlain, Port: 9092, Type: "internal", TLS: false},
					{Name: common.ListenerTLS, Port: 9093, Type: "internal", TLS: true},
				},
				Config: config,
			},
			EntityOperator: &kafkav1.EntityOperatorSpec{
				TopicOperator: &kafkav1.EntityTopicOperatorSpec{},
				UserOperator:  &kafkav1.EntityUserOperatorSpec{},
			},
		},
	}, nil
}

// Sync creates/updates the Strimzi KafkaNodePool and Kafka CRs for dualRole.
func (p *Provider) Sync(c *controller.Context) error {
	l := log.FromContext(c.Context())
	l.Info("Syncing instance", "name", c.Name())

	engine, ok := c.Instance().Spec.Components[common.ComponentEngine]
	if !ok {
		return fmt.Errorf("spec.components[%s]: required", common.ComponentEngine)
	}
	if engine.Replicas == nil || engine.Storage == nil {
		return fmt.Errorf("spec.components[%s]: replicas and storage are required", common.ComponentEngine)
	}

	pool := buildNodePool(c, engine)
	kafka, err := buildKafka(c, engine)
	if err != nil {
		return err
	}

	// Apply the node pool before the Kafka CR so the operator can see the pool
	// when it reconciles the cluster (Strimzi deploy docs apply pools first).
	if err := c.Apply(pool); err != nil {
		return fmt.Errorf("apply KafkaNodePool: %w", err)
	}
	if err := c.Apply(kafka); err != nil {
		return fmt.Errorf("apply Kafka: %w", err)
	}
	return nil
}
