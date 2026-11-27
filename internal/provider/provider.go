package provider

import (
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	kafkav1 "github.com/openeverest/provider-strimzi-kafka/api/kafka.strimzi.io/v1"
	"github.com/openeverest/provider-strimzi-kafka/internal/common"
)

// Compile-time check that Provider implements the required interface.
var _ controller.ProviderInterface = (*Provider)(nil)

// Provider implements controller.ProviderInterface for the provider-strimzi-kafka provider.
type Provider struct {
	controller.BaseProvider
}

// New creates a new Provider instance.
func New() *Provider {
	return &Provider{
		BaseProvider: controller.BaseProvider{
			ProviderName: common.ProviderName,
			SchemeFuncs: []func(*runtime.Scheme) error{
				kafkav1.AddToScheme,
			},
			WatchConfigs: []controller.WatchConfig{
				controller.WatchOwned(&kafkav1.Kafka{}),
				controller.WatchOwned(&kafkav1.KafkaNodePool{}),
			},
		},
	}
}

// Cleanup handles deletion of provider-managed resources.
func (p *Provider) Cleanup(c *controller.Context) error {
	l := log.FromContext(c.Context())
	l.Info("Cleaning up instance", "name", c.Name())

	// Resources with owner references set via c.Apply() are automatically
	// garbage collected. Only implement this if you need custom cleanup.
	return nil
}
