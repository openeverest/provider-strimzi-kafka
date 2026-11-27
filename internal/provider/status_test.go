package provider

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kafkav1 "github.com/openeverest/provider-strimzi-kafka/api/kafka.strimzi.io/v1"
	"github.com/openeverest/provider-strimzi-kafka/internal/common"
)

func TestSplitHostPort(t *testing.T) {
	t.Parallel()
	host, port := splitHostPort("my-kafka-kafka-bootstrap.ns.svc:9092")
	assert.Equal(t, "my-kafka-kafka-bootstrap.ns.svc", host)
	assert.Equal(t, "9092", port)

	host, port = splitHostPort("a:9092,b:9092")
	assert.Equal(t, "a", host)
	assert.Equal(t, "9092", port)
}

func TestConnectionDetailsFromKafka(t *testing.T) {
	t.Parallel()
	kafka := &kafkav1.Kafka{
		Status: kafkav1.KafkaStatus{
			Listeners: []kafkav1.ListenerStatus{
				{Name: common.ListenerTLS, BootstrapServers: "tls-bootstrap:9093"},
				{Name: common.ListenerPlain, BootstrapServers: "plain-bootstrap.ns.svc:9092"},
			},
		},
	}
	details, ok := connectionDetailsFromKafka(kafka)
	require.True(t, ok)
	assert.Equal(t, "kafka", details.Type)
	assert.Equal(t, common.ProviderName, details.Provider)
	assert.Equal(t, "plain-bootstrap.ns.svc", details.Host)
	assert.Equal(t, "9092", details.Port)
	assert.Equal(t, "plain-bootstrap.ns.svc:9092", details.URI)
	assert.Equal(t, "plain-bootstrap.ns.svc:9092", details.AdditionalProperties["bootstrapServers"])
}

func TestConnectionDetailsFromKafka_MissingPlain(t *testing.T) {
	t.Parallel()
	kafka := &kafkav1.Kafka{
		Status: kafkav1.KafkaStatus{
			Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue}},
			Listeners:  []kafkav1.ListenerStatus{{Name: common.ListenerTLS, BootstrapServers: "x:9093"}},
		},
	}
	_, ok := connectionDetailsFromKafka(kafka)
	assert.False(t, ok)
}
