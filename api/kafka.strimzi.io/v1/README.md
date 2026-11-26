# Strimzi API types (`kafka.strimzi.io/v1`)

Owned Go types for the Strimzi resources this provider reconciles. There is no
official Strimzi Go SDK; we maintain a **minimal subset** of the CRD surface and
grow it as Sync/Status need more fields.

## Conventions

- Match field names and JSON tags to the pinned Strimzi CRDs (chart dependency
  `strimzi-kafka-operator` in `charts/provider-strimzi-kafka/Chart.yaml`).
- Prefer pointers for optional nested objects so server-side apply omits them.
- After editing types, run `make generate` (runs `controller-gen` for DeepCopy).

## Kinds covered today

| Kind | Purpose |
|------|---------|
| `Kafka` | Cluster definition (listeners, version, entity operator) |
| `KafkaNodePool` | Broker/controller pools and storage |

Add new kinds (e.g. `KafkaTopic`, `KafkaUser`) in this package when needed.
