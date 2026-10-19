import logging
import os
from typing import Any

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

logger = logging.getLogger(__name__)


def init_telemetry(app: Any, service_name: str = "order_service") -> None:
    from app.config import settings

    otlp_endpoint = (
        settings.otel_exporter_otlp_endpoint
        or os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
    ).strip()

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)

    if otlp_endpoint:
        try:
            url = f"{otlp_endpoint.rstrip('/')}/v1/traces"
            exporter = OTLPSpanExporter(endpoint=url)
            processor = BatchSpanProcessor(exporter)
            provider.add_span_processor(processor)
            logger.info("OpenTelemetry OTLP endpoint: %s", otlp_endpoint)
        except Exception as exc:
            logger.warning("Failed to configure OTLPSpanExporter: %s", exc)

    trace.set_tracer_provider(provider)
    FastAPIInstrumentor.instrument_app(app, tracer_provider=provider)


def get_current_traceparent() -> str | None:
    current_span = trace.get_current_span()
    ctx = current_span.get_span_context()
    if ctx.is_valid:
        return f"00-{ctx.trace_id:032x}-{ctx.span_id:016x}-{ctx.trace_flags:02x}"
    return None


def inject_traceparent(carrier: dict[str, Any]) -> None:
    TraceContextTextMapPropagator().inject(carrier)


def extract_traceparent(carrier: dict[str, Any]) -> Any:
    return TraceContextTextMapPropagator().extract(carrier)
