"""Monitoring: structured logging and metrics collection."""

from bq_entity_resolution.monitoring.logging import setup_logging
from bq_entity_resolution.monitoring.metrics import MetricsCollector

__all__ = [
    "MetricsCollector",
    "setup_logging",
]
