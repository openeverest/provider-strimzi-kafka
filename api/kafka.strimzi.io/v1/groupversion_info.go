package v1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is the API group and version for these types.
	GroupVersion = schema.GroupVersion{Group: "kafka.strimzi.io", Version: "v1"}

	// SchemeBuilder registers this group's types with a scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds this group's types to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)
