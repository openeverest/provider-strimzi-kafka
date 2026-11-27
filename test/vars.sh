#!/bin/bash

# Environment variables sourced by the `make test-integration*` targets before
# running chainsaw. Use it to pin the operator/engine versions your tests run
# against so they are reproducible locally and in CI.
#
# Reference values in chainsaw test files via ($values) bindings or plain
# environment substitution in `script:` steps.

export PROVIDER_ROOT_PATH=${PROVIDER_ROOT_PATH:-${PWD}}
echo "PROVIDER_ROOT_PATH=${PROVIDER_ROOT_PATH}"

# Strimzi Cluster Operator chart/app version (charts/provider-strimzi-kafka/Chart.yaml).
export STRIMZI_OPERATOR_VERSION=${STRIMZI_OPERATOR_VERSION:-"1.3.1"}
echo "STRIMZI_OPERATOR_VERSION=${STRIMZI_OPERATOR_VERSION}"
