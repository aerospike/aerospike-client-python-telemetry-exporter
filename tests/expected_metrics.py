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
        "aerospike_client_command_count",
        "aerospike_client_command_retries",
        "aerospike_client_cpu_percent",
        "aerospike_client_feature_api_background",
        "aerospike_client_feature_api_blocking",
        "aerospike_client_feature_api_deferred",
        "aerospike_client_feature_filter_ael",
        "aerospike_client_feature_filter_exp",
        "aerospike_client_feature_shape_batch",
        "aerospike_client_feature_shape_point",
        "aerospike_client_feature_shape_query",
        "aerospike_client_invalid_node_count",
        "aerospike_client_latency_bucket",
        "aerospike_client_memory_bytes",
        "aerospike_client_namespace_bytes_in",
        "aerospike_client_namespace_bytes_out",
        "aerospike_client_namespace_device_overload",
        "aerospike_client_namespace_errors",
        "aerospike_client_namespace_key_busy",
        "aerospike_client_namespace_latency_bucket",
        "aerospike_client_namespace_record_too_big",
        "aerospike_client_namespace_server_timeouts",
        "aerospike_client_node_conn_latency_bucket",
        "aerospike_client_node_connection_auth_failure",
        "aerospike_client_node_connection_closed_error",
        "aerospike_client_node_connection_closed_idle",
        "aerospike_client_node_connection_closed_node_removed",
        "aerospike_client_node_connection_open_failure",
        "aerospike_client_node_connection_tls_handshake_failure",
        "aerospike_client_node_connections_closed",
        "aerospike_client_node_connections_in_pool",
        "aerospike_client_node_connections_in_use",
        "aerospike_client_node_connections_opened",
        "aerospike_client_nodes_active",
        "aerospike_client_recover_queue_size",
    }
)
