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

"""Prometheus scrape reader and the HTTP server started by the exporter.

``CustomPrometheusMetricReader`` converts OpenTelemetry metrics into a
Prometheus registry. It does not open a port. ``OtelExporter.start_http_server``
builds a ``PrometheusHTTPServer`` for that registry, in plaintext, TLS, or
mutual TLS. ``MeterProvider.shutdown`` then stops that server. A webapp that
already serves Prometheus passes its registry to the reader and calls
``prometheus_client.start_http_server`` itself, with the same TLS fields.
``MeterProvider.shutdown`` does not stop that webapp server. This module does
not register process signal handlers.

Importing ``aerospike_client_telemetry.otel`` does not import this module.
"""

from __future__ import annotations

import logging
import os
import re
import socket
import ssl
import threading
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from itertools import chain
from json import dumps
from typing import Any

try:
    from prometheus_client import CollectorRegistry
    from prometheus_client import start_http_server as _start_http_server
    from prometheus_client.core import (
        CounterMetricFamily,
        GaugeMetricFamily,
        HistogramMetricFamily,
        InfoMetricFamily,
    )
    from prometheus_client.core import Metric as PrometheusMetric
except ImportError as exc:
    raise ImportError(
        "Prometheus support requires the prometheus extra: "
        'pip install "aerospike-client-telemetry-exporter[otel,prometheus]"'
    ) from exc

from opentelemetry.sdk.metrics import (
    Counter,
    ObservableCounter,
    ObservableGauge,
    ObservableUpDownCounter,
    UpDownCounter,
)
from opentelemetry.sdk.metrics import Histogram as HistogramInstrument
from opentelemetry.sdk.metrics.export import (
    AggregationTemporality,
    Gauge,
    Histogram,
    HistogramDataPoint,
    Metric,
    MetricReader,
    MetricsData,
    Sum,
)

_logger = logging.getLogger(__name__)

_SANITIZE_NAME = re.compile(r"[^a-zA-Z0-9:]+")
_SANITIZE_LABEL = re.compile(r"[^a-zA-Z0-9]+")
_UNIT_ANNOTATION = re.compile(r"{.*}")
_UNIT_MAPPINGS = {
    "d": "days",
    "h": "hours",
    "min": "minutes",
    "s": "seconds",
    "ms": "milliseconds",
    "us": "microseconds",
    "ns": "nanoseconds",
    "By": "bytes",
    "KiBy": "kibibytes",
    "MiBy": "mebibytes",
    "GiBy": "gibibytes",
    "TiBy": "tibibytes",
    "KBy": "kilobytes",
    "MBy": "megabytes",
    "GBy": "gigabytes",
    "TBy": "terabytes",
    "m": "meters",
    "V": "volts",
    "A": "amperes",
    "J": "joules",
    "W": "watts",
    "g": "grams",
    "Cel": "celsius",
    "Hz": "hertz",
    "1": "",
    "%": "percent",
}
_PER_UNIT_MAPPINGS = {
    "s": "second",
    "m": "minute",
    "h": "hour",
    "d": "day",
    "w": "week",
    "mo": "month",
    "y": "year",
}
_SCOPE_NAME = "otel_scope_name"
_SCOPE_VERSION = "otel_scope_version"
_SCOPE_SCHEMA_URL = "otel_scope_schema_url"
_TARGET_INFO = "target"
_DEFAULT_HOST = "127.0.0.1"
_DEFAULT_PORT = 9464


def _sanitize_name(name: str) -> str:
    """Match opentelemetry-exporter-prometheus 0.66b1 name sanitizing."""
    if name and name[0].isdigit():
        name = "_" + name[1:]
    return _SANITIZE_NAME.sub("_", name)


def _sanitize_unit_token(name: str) -> str:
    return _SANITIZE_NAME.sub("_", name)


def _sanitize_label(name: str) -> str:
    if name and name[0].isdigit():
        name = "_" + name[1:]
    return _SANITIZE_LABEL.sub("_", name)


def _map_unit(unit: str) -> str:
    unit = _UNIT_ANNOTATION.sub("", unit)
    if unit in _UNIT_MAPPINGS:
        return _UNIT_MAPPINGS[unit]
    parts = unit.split("/", maxsplit=1)
    if len(parts) == 2:
        bottom = _sanitize_unit_token(parts[1])
        if bottom:
            top = _sanitize_unit_token(parts[0])
            top = _UNIT_MAPPINGS.get(top, top)
            bottom = _PER_UNIT_MAPPINGS.get(bottom, bottom)
            return f"{top}_per_{bottom}" if top else f"per_{bottom}"
    return _sanitize_unit_token(unit).strip("_")


def _label_value(value: Any) -> str:
    if isinstance(value, str):
        return value
    return dumps(value, default=str)


def _sum_is_gauge(metric: Metric) -> bool:
    return (
        isinstance(metric.data, Sum)
        and not metric.data.is_monotonic
        and metric.data.aggregation_temporality == AggregationTemporality.CUMULATIVE
    )


def _convert_buckets(
    bucket_counts: Sequence[int], explicit_bounds: Sequence[float]
) -> list[tuple[str, int]]:
    buckets: list[tuple[str, int]] = []
    total = 0
    for upper_bound, count in zip(chain(explicit_bounds, ["+Inf"]), bucket_counts):
        total += count
        buckets.append((f"{upper_bound}", total))
    return buckets


def _require_file(path: str | None, label: str) -> None:
    if not isinstance(path, str) or not path:
        raise ValueError(f"{label} must be a file path")
    if not os.path.isfile(path):
        raise ValueError(f"{label} does not exist: {path}")


@dataclass(frozen=True)
class TLSConfig:
    """TLS material passed to ``prometheus_client.start_http_server``."""

    certfile: str | None = None
    keyfile: str | None = None
    client_cafile: str | None = None
    client_capath: str | None = None
    client_auth_required: bool = False
    tls_min_version: ssl.TLSVersion | None = None
    tls_max_version: ssl.TLSVersion | None = None


@dataclass(frozen=True)
class PrometheusConfig:
    """Bind address and TLS settings for ``PrometheusHTTPServer``."""

    host: str = _DEFAULT_HOST
    port: int = _DEFAULT_PORT
    tls: TLSConfig | None = None


def _validate_address(host: str, port: int) -> None:
    if not isinstance(host, str) or not host.strip():
        raise ValueError("host must be a non-empty bind address")
    if isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65535:
        raise ValueError(f"port must be an integer from 0 to 65535, got {port!r}")
    try:
        socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise ValueError(f"invalid Prometheus bind address {host}:{port}: {exc}") from exc


def _validate_tls(tls: TLSConfig | None) -> None:
    if tls is None:
        return
    if not isinstance(tls, TLSConfig):
        raise TypeError("tls_config must be a TLSConfig")
    if not isinstance(tls.client_auth_required, bool):
        raise TypeError("client_auth_required must be a bool")
    for label, version in (
        ("tls_min_version", tls.tls_min_version),
        ("tls_max_version", tls.tls_max_version),
    ):
        if version is not None and not isinstance(version, ssl.TLSVersion):
            raise TypeError(f"{label} must be an ssl.TLSVersion")
    if (
        tls.tls_min_version is not None
        and tls.tls_max_version is not None
        and int(tls.tls_min_version) >= 0
        and int(tls.tls_max_version) >= 0
        and tls.tls_min_version > tls.tls_max_version
    ):
        raise ValueError("tls_min_version is newer than tls_max_version")
    if tls.client_auth_required and not tls.client_cafile and not tls.client_capath:
        raise ValueError("mutual TLS requires client_cafile or client_capath")
    has_cert = bool(tls.certfile) or bool(tls.keyfile)
    has_client = bool(tls.client_cafile) or bool(tls.client_capath) or tls.client_auth_required
    has_bounds = tls.tls_min_version is not None or tls.tls_max_version is not None
    if has_client or has_bounds or has_cert:
        if not tls.certfile or not tls.keyfile:
            raise ValueError("TLS requires both certfile and keyfile")
    if tls.certfile:
        _require_file(tls.certfile, "certfile")
    if tls.keyfile:
        _require_file(tls.keyfile, "keyfile")
    if tls.client_cafile:
        _require_file(tls.client_cafile, "client_cafile")
    if tls.client_capath:
        if not isinstance(tls.client_capath, str) or not os.path.isdir(tls.client_capath):
            raise ValueError(f"client_capath is not a directory: {tls.client_capath}")
    if not tls.certfile:
        return
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    try:
        if tls.tls_min_version is not None:
            context.minimum_version = tls.tls_min_version
        if tls.tls_max_version is not None:
            context.maximum_version = tls.tls_max_version
    except ValueError as exc:
        raise ValueError(f"invalid TLS version range: {exc}") from exc
    if tls.client_cafile or tls.client_capath:
        try:
            context.load_verify_locations(tls.client_cafile, tls.client_capath)
        except OSError as exc:
            raise ValueError(f"cannot load client CA certificates: {exc}") from exc
    try:
        context.load_cert_chain(tls.certfile, tls.keyfile)
    except OSError as exc:
        raise ValueError(f"cannot load server certificate or key: {exc}") from exc


def _probe_host(host: str) -> str:
    if host in {"0.0.0.0", "::"}:
        return "::1" if host == "::" else "127.0.0.1"
    return host


class PrometheusHTTPServer:
    """HTTP listener for one Prometheus registry.

    Construction validates the address and TLS material and does not bind.
    ``start()`` calls ``prometheus_client.start_http_server``. ``shutdown()``
    closes the socket and joins the server thread. Both calls are safe to
    repeat. A port that is already taken raises ``OSError`` and leaves no
    thread behind. Each process binds its own port; this server does not
    combine metrics from other processes.
    """

    def __init__(
        self,
        registry: CollectorRegistry,
        host: str = _DEFAULT_HOST,
        port: int = _DEFAULT_PORT,
        tls_config: TLSConfig | None = None,
        *,
        config: PrometheusConfig | None = None,
    ) -> None:
        if not isinstance(registry, CollectorRegistry):
            raise TypeError("registry must be a CollectorRegistry")
        if config is not None:
            if host is not _DEFAULT_HOST or port != _DEFAULT_PORT or tls_config is not None:
                raise TypeError("pass config or host, port, and tls_config, not both")
            host = config.host
            port = config.port
            tls_config = config.tls
        _validate_address(host, port)
        _validate_tls(tls_config)
        self._registry = registry
        self._host = host
        self._configured_port = port
        self._bound_port = port
        self._tls = tls_config
        self._httpd: Any = None
        self._thread: threading.Thread | None = None
        self._started = False
        self._stopped = False
        self._lock = threading.RLock()

    @property
    def registry(self) -> CollectorRegistry:
        return self._registry

    @property
    def port(self) -> int:
        """Configured port before ``start()``, then the bound port."""
        return self._bound_port

    def start(self) -> None:
        """Bind the scrape port. Raise if this server is already running or stopped."""
        with self._lock:
            if self._stopped:
                raise RuntimeError("Prometheus HTTP server is already shut down")
            if self._started:
                raise RuntimeError("Prometheus HTTP server is already started")
            _validate_address(self._host, self._configured_port)
            _validate_tls(self._tls)
            tls = self._tls or TLSConfig()
            try:
                httpd, thread = _start_http_server(
                    self._configured_port,
                    addr=self._host,
                    registry=self._registry,
                    certfile=tls.certfile,
                    keyfile=tls.keyfile,
                    client_cafile=tls.client_cafile,
                    client_capath=tls.client_capath,
                    client_auth_required=tls.client_auth_required,
                    tls_min_version=tls.tls_min_version,
                    tls_max_version=tls.tls_max_version,
                )
            except OSError as exc:
                raise OSError(
                    exc.errno or 0,
                    f"Prometheus HTTP server cannot bind {self._host}:{self._configured_port}: "
                    f"{exc.strerror or exc}. Each process needs its own port; "
                    "this server does not aggregate metrics from other workers.",
                ) from exc
            self._httpd = httpd
            self._thread = thread
            self._bound_port = int(httpd.server_address[1])
            self._started = True
            self._wait_until_listening()

    def shutdown(self, timeout_millis: float = 30_000) -> None:
        """Stop serving, close the socket, and join the server thread."""
        with self._lock:
            if self._stopped:
                return
            self._stopped = True
            httpd = self._httpd
            thread = self._thread
            self._httpd = None
            self._thread = None
        if httpd is None and thread is None:
            return
        error: Exception | None = None
        try:
            if httpd is not None:
                httpd.shutdown()
        except Exception as exc:
            error = exc
        if httpd is not None:
            try:
                httpd.server_close()
            except Exception as exc:
                error = error or exc
        if thread is not None:
            thread.join(timeout_millis / 1000)
            if thread.is_alive():
                error = TimeoutError(
                    "Prometheus HTTP server thread did not stop within "
                    f"{timeout_millis} milliseconds"
                )
        if error is not None:
            raise error

    def _wait_until_listening(self) -> None:
        deadline = time.monotonic() + 5
        probe = _probe_host(self._host)
        while time.monotonic() < deadline:
            try:
                with socket.create_connection((probe, self._bound_port), timeout=0.2):
                    return
            except OSError:
                time.sleep(0.01)
        self.shutdown()
        raise TimeoutError(
            f"Prometheus HTTP server did not accept connections on {probe}:{self._bound_port}"
        )


class _Collector:
    def __init__(
        self,
        lock: threading.RLock,
        callback: Callable[[], None],
        disable_target_info: bool,
        prefix: str,
        scope_info_enabled: bool,
        resource_attribute_filter: Callable[[str], bool] | None,
    ) -> None:
        self._lock = lock
        self._callback = callback
        self._metrics_data: MetricsData | None = None
        self._closed = False
        self._disable_target_info = disable_target_info
        self._prefix = prefix
        self._scope_info_enabled = scope_info_enabled
        self._resource_attribute_filter = resource_attribute_filter

    def set_metrics_data(self, metrics_data: MetricsData) -> None:
        self._metrics_data = metrics_data

    def close(self) -> None:
        self._closed = True
        self._metrics_data = None

    def collect(self) -> Iterable[PrometheusMetric]:
        with self._lock:
            if self._closed:
                return []
            self._callback()
            metrics_data = self._metrics_data
            if metrics_data is None:
                return []
            return list(self._translate(metrics_data))

    def _translate(self, metrics_data: MetricsData) -> Iterable[PrometheusMetric]:
        families: dict[str, PrometheusMetric] = {}
        if not self._disable_target_info:
            attributes: dict[str, Any] = {}
            for resource_metrics in metrics_data.resource_metrics:
                attributes.update(resource_metrics.resource.attributes)
            labels = {_sanitize_label(key): _label_value(value) for key, value in attributes.items()}
            info = InfoMetricFamily(_TARGET_INFO, "Target metadata", labels=labels)
            info.add_metric(list(labels), labels)
            families[_TARGET_INFO] = info
        for resource_metrics in metrics_data.resource_metrics:
            resource_attrs = self._resource_attributes(resource_metrics.resource.attributes)
            for scope_metrics in resource_metrics.scope_metrics:
                scope_attrs = self._scope_attributes(scope_metrics.scope)
                for metric in scope_metrics.metrics:
                    self._translate_metric(families, metric, resource_attrs, scope_attrs)
        return families.values()

    def _resource_attributes(self, attributes: dict[str, Any]) -> dict[str, Any]:
        if self._resource_attribute_filter is None:
            return {}
        return {
            key: value
            for key, value in attributes.items()
            if self._resource_attribute_filter(key)
        }

    def _scope_attributes(self, scope: Any) -> dict[str, Any]:
        if not self._scope_info_enabled:
            return {}
        attributes = {
            f"otel_scope_{key}": value for key, value in (scope.attributes or {}).items()
        }
        attributes[_SCOPE_NAME] = scope.name or ""
        attributes[_SCOPE_VERSION] = scope.version or ""
        attributes[_SCOPE_SCHEMA_URL] = scope.schema_url or ""
        return attributes

    def _translate_metric(
        self,
        families: dict[str, PrometheusMetric],
        metric: Metric,
        resource_attrs: dict[str, Any],
        scope_attrs: dict[str, Any],
    ) -> None:
        name = _sanitize_name(f"{self._prefix}_{metric.name}" if self._prefix else metric.name)
        description = metric.description or ""
        unit = _map_unit(metric.unit or "")
        supported = isinstance(metric.data, (Sum, Gauge, Histogram))
        if not supported:
            _logger.warning(
                "Unsupported metric data %s for %s", type(metric.data).__name__, metric.name
            )
            return
        per_metric_id = "|".join((name, description, unit))
        labels, rows, values = self._points(metric.data, resource_attrs, scope_attrs)
        if isinstance(metric.data, Sum) and not _sum_is_gauge(metric):
            family = _family(
                families,
                "|".join((per_metric_id, CounterMetricFamily.__name__)),
                CounterMetricFamily,
                name,
                description,
                labels,
                unit,
            )
            for row, value in zip(rows, values):
                family.add_metric(row, value)
            return
        if isinstance(metric.data, Gauge) or _sum_is_gauge(metric):
            family = _family(
                families,
                "|".join((per_metric_id, GaugeMetricFamily.__name__)),
                GaugeMetricFamily,
                name,
                description,
                labels,
                unit,
            )
            for row, value in zip(rows, values):
                family.add_metric(row, value)
            return
        if isinstance(metric.data, Histogram):
            family = _family(
                families,
                "|".join((per_metric_id, HistogramMetricFamily.__name__)),
                HistogramMetricFamily,
                name,
                description,
                labels,
                unit,
            )
            for row, value in zip(rows, values):
                family.add_metric(
                    row,
                    buckets=_convert_buckets(value["bucket_counts"], value["explicit_bounds"]),
                    sum_value=value["sum"],
                )
            return

    @staticmethod
    def _points(
        data: Any, resource_attrs: dict[str, Any], scope_attrs: dict[str, Any]
    ) -> tuple[list[str], list[list[str]], list[Any]]:
        keys: set[str] = set()
        rows: list[dict[str, str]] = []
        values: list[Any] = []
        for point in data.data_points:
            labels: dict[str, str] = {}
            for key, value in chain(
                resource_attrs.items(), scope_attrs.items(), point.attributes.items()
            ):
                label = _sanitize_label(key)
                keys.add(label)
                labels[label] = _label_value(value)
            rows.append(labels)
            if isinstance(point, HistogramDataPoint):
                values.append(
                    {
                        "bucket_counts": point.bucket_counts,
                        "explicit_bounds": point.explicit_bounds,
                        "sum": point.sum,
                    }
                )
            else:
                values.append(point.value)
        label_keys = sorted(keys)
        return (
            label_keys,
            [[row.get(label, "") for label in label_keys] for row in rows],
            values,
        )


def _family(
    families: dict[str, PrometheusMetric],
    family_id: str,
    factory: Any,
    name: str,
    description: str,
    labels: Sequence[str],
    unit: str,
) -> PrometheusMetric:
    if family_id not in families:
        families[family_id] = factory(name, description, labels=labels, unit=unit)
    return families[family_id]


class CustomPrometheusMetricReader(MetricReader):
    """Read OpenTelemetry metrics into a Prometheus registry.

    The constructor accepts conversion settings and a ``CollectorRegistry``.
    It does not accept an HTTP server and does not bind a socket. Call
    ``OtelExporter.start_http_server`` to serve this registry.
    ``MeterProvider.shutdown()`` calls ``shutdown()``, which unregisters the
    collector and stops the server that method started. This class does not
    install SIGTERM or SIGINT handlers.
    """

    def __init__(
        self,
        disable_target_info: bool = False,
        prefix: str = "",
        scope_info_enabled: bool = True,
        *,
        resource_attribute_filter: Callable[[str], bool] | None = None,
        registry: CollectorRegistry | None = None,
    ) -> None:
        super().__init__(
            preferred_temporality={
                Counter: AggregationTemporality.CUMULATIVE,
                UpDownCounter: AggregationTemporality.CUMULATIVE,
                HistogramInstrument: AggregationTemporality.CUMULATIVE,
                ObservableCounter: AggregationTemporality.CUMULATIVE,
                ObservableUpDownCounter: AggregationTemporality.CUMULATIVE,
                ObservableGauge: AggregationTemporality.CUMULATIVE,
            }
        )
        if registry is not None and not isinstance(registry, CollectorRegistry):
            raise TypeError("registry must be a CollectorRegistry")
        if registry is None:
            registry = CollectorRegistry()
        self._lock = threading.RLock()
        self._shutdown_cv = threading.Condition(self._lock)
        self._shutdown_started = False
        self._shutdown_done = False
        self._collector = _Collector(
            self._lock,
            self.collect,
            disable_target_info,
            prefix,
            scope_info_enabled,
            resource_attribute_filter,
        )
        self._registry = registry
        self._http_server: PrometheusHTTPServer | None = None
        self._registry.register(self._collector)

    @property
    def registry(self) -> CollectorRegistry:
        return self._registry

    def _attach_http_server(self, server: PrometheusHTTPServer) -> None:
        """Remember the server ``OtelExporter.start_http_server`` started."""
        if not isinstance(server, PrometheusHTTPServer):
            raise TypeError("server must be a PrometheusHTTPServer")
        if server.registry is not self._registry:
            raise ValueError("Prometheus HTTP server must use this reader's CollectorRegistry")
        with self._lock:
            if self._shutdown_done or self._shutdown_started:
                raise RuntimeError("CustomPrometheusMetricReader is already shut down")
            if self._http_server is not None:
                raise RuntimeError("Prometheus HTTP server is already started for this reader")
            self._http_server = server

    def _receive_metrics(
        self,
        metrics_data: MetricsData,
        timeout_millis: float = 10_000,
        **kwargs: Any,
    ) -> None:
        if metrics_data is None:
            return
        with self._lock:
            if self._collector._closed:
                return
            self._collector.set_metrics_data(metrics_data)

    def shutdown(self, timeout_millis: float = 30_000, **kwargs: Any) -> None:
        """Unregister the collector and stop the server the exporter started.

        The collector is removed even when HTTP shutdown fails. A second or
        overlapping call waits for the first attempt and then returns.
        """
        with self._lock:
            if self._shutdown_done:
                return
            if self._shutdown_started:
                while not self._shutdown_done:
                    self._shutdown_cv.wait()
                return
            self._shutdown_started = True
            self._collector.close()
            server = self._http_server
        error: Exception | None = None
        try:
            try:
                self._registry.unregister(self._collector)
            except KeyError:
                pass
            if server is not None:
                server.shutdown(timeout_millis=timeout_millis)
        except Exception as exc:
            error = exc
        with self._lock:
            self._shutdown_done = True
            self._shutdown_cv.notify_all()
        if error is not None:
            raise error


__all__ = [
    "CollectorRegistry",
    "CustomPrometheusMetricReader",
    "PrometheusConfig",
    "PrometheusHTTPServer",
    "TLSConfig",
]
