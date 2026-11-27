package provider

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller/validationtest"

	"github.com/openeverest/provider-strimzi-kafka/internal/common"
)

func TestValidate_AcceptsOddReplicas(t *testing.T) {
	t.Parallel()
	result := New().Validate(testValidateInput(t, 3, "10Gi", nil))
	assert.Empty(t, result.Errors)
}

func TestValidate_RejectsEvenReplicas(t *testing.T) {
	t.Parallel()
	result := New().Validate(testValidateInput(t, 2, "10Gi", nil))
	require.NotEmpty(t, result.Errors)
	assert.Contains(t, result.Errors.ToAggregate().Error(), "must be odd")
}

func TestValidate_RejectsService(t *testing.T) {
	t.Parallel()
	result := New().Validate(testValidateInput(t, 1, "10Gi", &corev1alpha1.Service{
		ServiceType: corev1.ServiceTypeLoadBalancer,
	}))
	require.NotEmpty(t, result.Errors)
	assert.Contains(t, result.Errors.ToAggregate().Error(), "service")
}

func testValidateInput(t *testing.T, replicas int32, storageSize string, svc *corev1alpha1.Service) controller.ValidationInput {
	t.Helper()
	size := resource.MustParse(storageSize)
	inst := &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: corev1alpha1.InstanceSpec{
			Version: "4.3.2",
			Components: map[string]corev1alpha1.ComponentSpec{
				common.ComponentEngine: {
					Type:     common.ComponentTypeKafka,
					Replicas: &replicas,
					Storage:  &corev1alpha1.Storage{Size: size},
					Service:  svc,
				},
			},
		},
	}
	return controller.ValidationInput{Instance: inst}
}

// TestValidate runs the checks the runtime runs before every Sync: the
// schemas, supportedFields and catalog this provider publishes, then Validate.
func TestValidate(t *testing.T) {
	t.Parallel()
	v := validationtest.New(t, New(), "../../charts/provider-strimzi-kafka/generated/provider-spec.yaml")

	tests := []struct {
		fixture string
		// want lists the field paths of the expected errors.
		want []string
	}{
		{fixture: "testdata/validate/minimal.yaml"},
		// TODO: Add a fixture per rule, e.g.:
		// {
		//     fixture: "testdata/validate/even-replicas.yaml",
		//     want:    []string{"spec.components[engine].replicas"},
		// },
	}

	for _, tt := range tests {
		t.Run(tt.fixture, func(t *testing.T) {
			t.Parallel()
			res := v.Run(validationtest.Load(t, tt.fixture), nil)

			var got []string
			for _, err := range res.Errors {
				got = append(got, err.Field)
			}
			assert.Equal(t, tt.want, got, "errors: %v", res.Errors)
		})
	}
}
