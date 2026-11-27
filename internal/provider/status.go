package provider

import (
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	kafkav1 "github.com/openeverest/provider-strimzi-kafka/api/kafka.strimzi.io/v1"
	"github.com/openeverest/provider-strimzi-kafka/internal/common"
)

// Status maps Strimzi Kafka.status.conditions onto Instance status facts.
// The runtime derives phase (Pending/Provisioned/Terminating) and conditions.
//
// Strimzi Cluster Operator (KafkaAssemblyOperator) writes:
//   - type=Ready, status=True on successful reconcile
//   - type=NotReady, status=True with reason/message on failure / while creating
// See: https://github.com/strimzi/strimzi-kafka-operator (KafkaAssemblyOperator.createOrUpdate)
func (p *Provider) Status(c *controller.Context) (controller.Status, error) {
	l := log.FromContext(c.Context())
	l.Info("Computing status", "name", c.Name())

	kafka := &kafkav1.Kafka{}
	if err := c.Get(kafka, c.Name()); err != nil {
		if controller.IsNotFound(err) {
			return withDesiredReplicas(c, controller.Progressing("Waiting for Kafka resource")), nil
		}
		return controller.Status{}, err
	}

	if ready := meta.FindStatusCondition(kafka.Status.Conditions, "Ready"); ready != nil && ready.Status == metav1.ConditionTrue {
		details, ok := connectionDetailsFromKafka(kafka)
		if !ok {
			return withDesiredReplicas(c, controller.Progressing("Kafka is Ready; waiting for listener addresses")), nil
		}
		return withDesiredReplicas(c, controller.ReadyWithConnectionDetails(details)), nil
	}

	// Strimzi uses type=NotReady (status=True) while deploying and when a
	// reconcile fails; both are retried by the Cluster Operator, so surface
	// them as Progressing with the operator message rather than Failed.
	if notReady := meta.FindStatusCondition(kafka.Status.Conditions, "NotReady"); notReady != nil && notReady.Status == metav1.ConditionTrue {
		msg := notReady.Message
		if msg == "" {
			msg = notReady.Reason
		}
		if msg == "" {
			msg = "Kafka cluster is not ready"
		}
		return withDesiredReplicas(c, controller.Progressing(msg)), nil
	}

	return withDesiredReplicas(c, controller.Progressing("Waiting for Strimzi to report Kafka status")), nil
}

func withDesiredReplicas(c *controller.Context, st controller.Status) controller.Status {
	engine, ok := c.Instance().Spec.Components[common.ComponentEngine]
	if !ok || engine.Replicas == nil {
		return st
	}
	return st.WithDesiredReplicas(common.ComponentEngine, *engine.Replicas)
}

func connectionDetailsFromKafka(kafka *kafkav1.Kafka) (controller.ConnectionDetails, bool) {
	for _, listener := range kafka.Status.Listeners {
		if listener.Name != common.ListenerPlain || listener.BootstrapServers == "" {
			continue
		}
		host, port := splitHostPort(listener.BootstrapServers)
		details := controller.ConnectionDetails{
			Type:     "kafka",
			Provider: common.ProviderName,
			Host:     host,
			Port:     port,
			URI:      listener.BootstrapServers,
			AdditionalProperties: map[string]string{
				"bootstrapServers": listener.BootstrapServers,
			},
		}
		return details, true
	}
	return controller.ConnectionDetails{}, false
}

// splitHostPort takes the first host:port from a Strimzi bootstrapServers
// string (comma-separated). Port may be empty if absent.
func splitHostPort(bootstrap string) (host, port string) {
	first, _, _ := strings.Cut(bootstrap, ",")
	first = strings.TrimSpace(first)
	if first == "" {
		return "", ""
	}
	// Prefer LastIndex so IPv6-ish forms still yield a port when present.
	idx := strings.LastIndex(first, ":")
	if idx < 0 {
		return first, ""
	}
	return first[:idx], first[idx+1:]
}
