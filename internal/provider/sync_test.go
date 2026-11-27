package provider

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplicationSettings(t *testing.T) {
	t.Parallel()
	cases := []struct {
		replicas   int32
		wantRF     int32
		wantMinISR int32
	}{
		{1, 1, 1}, // kafka-single-node.yaml
		{3, 3, 2}, // kafka-with-dual-role-nodes.yaml
		{5, 3, 2}, // capped like the 3-node example defaults
	}
	for _, tc := range cases {
		rf, minISR := replicationSettings(tc.replicas)
		assert.Equal(t, tc.wantRF, rf, "replicas=%d", tc.replicas)
		assert.Equal(t, tc.wantMinISR, minISR, "replicas=%d", tc.replicas)
	}
}

func TestBuildBrokerConfig(t *testing.T) {
	t.Parallel()
	cfg, err := buildBrokerConfig(3)
	require.NoError(t, err)
	var m map[string]int32
	require.NoError(t, json.Unmarshal(cfg.Raw, &m))
	assert.Equal(t, int32(3), m["offsets.topic.replication.factor"])
	assert.Equal(t, int32(3), m["transaction.state.log.replication.factor"])
	assert.Equal(t, int32(2), m["transaction.state.log.min.isr"])
	assert.Equal(t, int32(3), m["default.replication.factor"])
	assert.Equal(t, int32(2), m["min.insync.replicas"])
}

func TestMetadataVersionFor(t *testing.T) {
	t.Parallel()
	mv, ok := metadataVersionFor("4.3.2")
	require.True(t, ok)
	assert.Equal(t, "4.3-IV0", mv)

	_, ok = metadataVersionFor("9.9.9")
	assert.False(t, ok)
}
