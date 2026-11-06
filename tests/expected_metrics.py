# Copyright 2026 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""Metric names the tests require, independent of the catalog.

Completeness checks compare published names to this set. They do not walk
``CATALOG``, so deleting a catalog entry fails the test instead of shrinking
the expectation.
"""

from __future__ import annotations

EXPECTED_METRIC_NAMES = frozenset(
    {
        "aerospike.client.cluster.batch_partial_failure",
        "aerospike.client.cluster.command_count",
        "aerospike.client.cluster.cpu_percent",
        "aerospike.client.cluster.delay_queue_timeout_count",
        "aerospike.client.cluster.invalid_node_count",
        "aerospike.client.cluster.latency_bucket",
        "aerospike.client.cluster.memory_bytes",
        "aerospike.client.cluster.open_connections",
        "aerospike.client.cluster.recover_queue_size",
        "aerospike.client.cluster.retry_count",
        "aerospike.client.cluster.total_nodes",
        "aerospike.client.cluster.validation_auth_failure",
        "aerospike.client.cluster.validation_open_failure",
        "aerospike.client.cluster.validation_tls_handshake_failure",
        "aerospike.client.feature.api_background",
        "aerospike.client.feature.api_blocking",
        "aerospike.client.feature.api_deferred",
        "aerospike.client.feature.filter_ael",
        "aerospike.client.feature.filter_exp",
        "aerospike.client.feature.object_mapping_read",
        "aerospike.client.feature.object_mapping_write",
        "aerospike.client.feature.shape_batch",
        "aerospike.client.feature.shape_point",
        "aerospike.client.feature.shape_query",
        "aerospike.client.namespace.bytes_in",
        "aerospike.client.namespace.bytes_out",
        "aerospike.client.namespace.client_timeouts",
        "aerospike.client.namespace.device_overload",
        "aerospike.client.namespace.errors",
        "aerospike.client.namespace.key_busy",
        "aerospike.client.namespace.latency_bucket",
        "aerospike.client.namespace.record_too_big",
        "aerospike.client.namespace.server_timeouts",
        "aerospike.client.node.conn_latency_bucket",
        "aerospike.client.node.connection_auth_failure",
        "aerospike.client.node.connection_open_failure",
        "aerospike.client.node.connection_tls_handshake_failure",
        "aerospike.client.node.connections_closed",
        "aerospike.client.node.connections_in_pool",
        "aerospike.client.node.connections_in_use",
        "aerospike.client.node.connections_opened",
    }
)
