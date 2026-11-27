package provider

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-strimzi-kafka/internal/common"
)

var (
	componentsPath = field.NewPath("spec", "components")
	enginePath     = componentsPath.Key(common.ComponentEngine)
)

// Validate checks Instance fields this provider realizes or must reject.
// Topology, component membership and version catalog membership are already
// enforced by the runtime against the Provider CR before Validate runs.
func (p *Provider) Validate(in controller.ValidationInput) controller.ValidationResult {
	instance := in.Instance
	engine, ok := instance.Spec.Components[common.ComponentEngine]
	if !ok {
		return controller.ValidationResult{Errors: field.ErrorList{
			field.Required(enginePath, fmt.Sprintf("required for dualRole topology")),
		}}
	}

	var errs field.ErrorList

	if engine.Replicas == nil {
		errs = append(errs, field.Required(enginePath.Child("replicas"), ""))
	} else {
		replicas := *engine.Replicas
		if replicas < 1 || replicas > 5 {
			errs = append(errs, field.Invalid(enginePath.Child("replicas"), replicas,
				"must be between 1 and 5"))
		}
		// KRaft controller quorum needs an odd number of controllers. Dual-role
		// nodes are controllers, so replicas must be odd (Strimzi dual-role /
		// single-node examples use 3 and 1).
		if replicas%2 == 0 {
			errs = append(errs, field.Invalid(enginePath.Child("replicas"), replicas,
				"must be odd for KRaft controller quorum"))
		}
	}

	if engine.Storage == nil || engine.Storage.Size.Cmp(resource.MustParse("0")) <= 0 {
		errs = append(errs, field.Required(enginePath.Child("storage", "size"), ""))
	}

	// Internal listeners only for the basic path — no Service exposure yet.
	if engine.Service != nil {
		errs = append(errs, field.Forbidden(enginePath.Child("service"),
			"not supported (cluster is exposed via Strimzi internal listeners)"))
	}
	if engine.SchedulingPolicy != nil {
		errs = append(errs, field.Forbidden(enginePath.Child("schedulingPolicy"), "not supported"))
	}
	// Strimzi maps images from Kafka.spec.kafka.version via STRIMZI_KAFKA_IMAGES;
	// a mismatched custom image is a common footgun (Strimzi configuring docs).
	if engine.Image != "" {
		errs = append(errs, field.Forbidden(enginePath.Child("image"),
			"not supported; set spec.version and let the Strimzi operator select the image"))
	}

	version := engine.Version
	if version == "" {
		version = instance.Spec.Version
	}
	if version == "" {
		version = "4.3.2"
	}
	if _, ok := metadataVersionFor(version); !ok {
		errs = append(errs, field.Invalid(field.NewPath("spec", "version"), version,
			"unsupported Kafka version (no metadata.version mapping)"))
	}

	return controller.ValidationResult{Errors: errs}
}
