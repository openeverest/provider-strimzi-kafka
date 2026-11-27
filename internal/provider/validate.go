package provider

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation/field"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-strimzi-kafka/internal/common"
)

var (
	componentsPath = field.NewPath("spec", "components")
	enginePath     = componentsPath.Key(common.ComponentEngine)
	versionPath    = field.NewPath("spec", "version")

	// maxStorageSize matches the dualRole UI disk max (topology.yaml).
	maxStorageSize = resource.MustParse("1000Gi")
)

// Validate checks Instance fields this provider realizes or must reject.
// Topology, component membership and version catalog membership are already
// enforced by the runtime against the Provider CR before Validate runs.
func (p *Provider) Validate(in controller.ValidationInput) controller.ValidationResult {
	instance := in.Instance
	engine, ok := instance.Spec.Components[common.ComponentEngine]
	if !ok {
		return controller.ValidationResult{Errors: field.ErrorList{
			field.Required(enginePath, "required for dualRole topology"),
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
	} else if engine.Storage.Size.Cmp(maxStorageSize) > 0 {
		errs = append(errs, field.Invalid(enginePath.Child("storage", "size"), engine.Storage.Size.String(),
			fmt.Sprintf("must be at most %s", maxStorageSize.String())))
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

	errs = append(errs, validateKafkaVersion(instance, engine)...)
	if in.Old != nil {
		errs = append(errs, validateImmutable(in.Old, instance)...)
	}

	return controller.ValidationResult{Errors: errs}
}

func validateKafkaVersion(instance *corev1alpha1.Instance, engine corev1alpha1.ComponentSpec) field.ErrorList {
	version, path := effectiveKafkaVersion(instance, engine)
	if _, ok := metadataVersionFor(version); ok {
		return nil
	}
	return field.ErrorList{
		field.Invalid(path, version, "unsupported Kafka version (no metadata.version mapping)"),
	}
}

// effectiveKafkaVersion mirrors resolveKafkaVersion for admission (no Context).
func effectiveKafkaVersion(instance *corev1alpha1.Instance, engine corev1alpha1.ComponentSpec) (string, *field.Path) {
	if engine.Version != "" {
		return engine.Version, enginePath.Child("version")
	}
	if instance.Spec.Version != "" {
		return instance.Spec.Version, versionPath
	}
	return common.DefaultKafkaVersion, versionPath
}

// validateImmutable rejects transitions that the capability matrix marks
// unsupported (horizontal scaling, version upgrades, storage changes).
func validateImmutable(oldInst, newInst *corev1alpha1.Instance) field.ErrorList {
	oldEngine, oldOK := oldInst.Spec.Components[common.ComponentEngine]
	newEngine, newOK := newInst.Spec.Components[common.ComponentEngine]
	if !oldOK || !newOK {
		return nil
	}

	var errs field.ErrorList

	if oldEngine.Replicas != nil && newEngine.Replicas != nil && *oldEngine.Replicas != *newEngine.Replicas {
		errs = append(errs, field.Invalid(enginePath.Child("replicas"), *newEngine.Replicas,
			"horizontal scaling is not supported"))
	}

	oldVer, _ := effectiveKafkaVersion(oldInst, oldEngine)
	newVer, newPath := effectiveKafkaVersion(newInst, newEngine)
	if oldVer != newVer {
		errs = append(errs, field.Invalid(newPath, newVer, "version upgrades are not supported"))
	}

	if oldEngine.Storage != nil && newEngine.Storage != nil {
		if oldEngine.Storage.Size.Cmp(newEngine.Storage.Size) != 0 {
			errs = append(errs, field.Invalid(enginePath.Child("storage", "size"), newEngine.Storage.Size.String(),
				"storage size changes are not supported"))
		}
		if storageClassValue(oldEngine.Storage) != storageClassValue(newEngine.Storage) {
			errs = append(errs, field.Invalid(enginePath.Child("storage", "storageClass"), storageClassValue(newEngine.Storage),
				"storage class changes are not supported"))
		}
	}

	return errs
}

func storageClassValue(s *corev1alpha1.Storage) string {
	if s == nil || s.StorageClass == nil {
		return ""
	}
	return *s.StorageClass
}
