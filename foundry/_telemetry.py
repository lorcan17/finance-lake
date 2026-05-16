"""Shared OTel MeterProvider factory for all Foundry pipeline services.

Each long-running entrypoint calls setup_meter(), instruments its work,
then calls provider.force_flush() + provider.shutdown() before exit.

OTEL_EXPORTER_OTLP_ENDPOINT controls the collector endpoint (default: localhost:4317).
OTEL_SDK_DISABLED=true disables export entirely (useful in tests / dev without a collector).
"""
from __future__ import annotations

import os

from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource


def setup_meter(service_name: str) -> tuple[MeterProvider, object]:
    """Return (provider, meter). Caller must flush+shutdown the provider on exit."""
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    exporter = OTLPMetricExporter(endpoint=endpoint)
    reader = PeriodicExportingMetricReader(exporter, export_interval_millis=30_000)
    resource = Resource.create({SERVICE_NAME: service_name})
    provider = MeterProvider(metric_readers=[reader], resource=resource)
    return provider, provider.get_meter(service_name)


def flush_and_shutdown(provider: MeterProvider) -> None:
    import logging
    try:
        provider.force_flush(timeout_millis=10_000)
        provider.shutdown()
    except Exception:
        logging.getLogger(__name__).warning("OTel flush/shutdown failed", exc_info=True)
