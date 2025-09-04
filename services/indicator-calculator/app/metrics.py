from prometheus_client import Counter, Gauge, generate_latest
from fastapi import APIRouter, Response

metrics_router = APIRouter()

# Define Prometheus metrics
events_total = Counter(
    "indicator_calculator_events_total",
    "Total number of events processed by indicator-calculator",
    ["event_type"],
)

status_last = Gauge(
    "indicator_calculator_status_last",
    "Last reported status of indicator-calculator",
    ["status_type"],
)

errors_total = Counter(
    "indicator_calculator_errors_total",
    "Total number of errors encountered by indicator-calculator",
)

# Indicator specific metrics
indicators_calculated_total = Counter(
    "indicator_calculator_indicators_calculated_total",
    "Total number of indicators calculated",
    ["indicator_name"],
)

documents_saved_total = Counter(
    "indicator_calculator_documents_saved_total",
    "Total number of documents saved to ArangoDB",
)

@metrics_router.get("/metrics")
async def get_metrics():
    return Response(content=generate_latest().decode("utf-8"), media_type="text/plain")
