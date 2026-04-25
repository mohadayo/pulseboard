import logging
import os
import time
from dataclasses import dataclass, field

from fastapi import FastAPI
from pydantic import BaseModel

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("analytics-api")

app = FastAPI(title="PulseBoard Analytics API", version="1.0.0")


class MetricPayload(BaseModel):
    service: str
    status: str
    response_time_ms: float
    timestamp: float | None = None


@dataclass
class MetricRecord:
    service: str
    status: str
    response_time_ms: float
    timestamp: float


@dataclass
class MetricsStore:
    records: list[MetricRecord] = field(default_factory=list)

    def add(self, record: MetricRecord) -> MetricRecord:
        self.records.append(record)
        logger.info("Recorded metric for service=%s status=%s", record.service, record.status)
        return record

    def get_all(self) -> list[MetricRecord]:
        return list(self.records)

    def get_by_service(self, service: str) -> list[MetricRecord]:
        return [r for r in self.records if r.service == service]

    def summary(self) -> dict:
        services: dict[str, dict] = {}
        for r in self.records:
            if r.service not in services:
                services[r.service] = {"total": 0, "healthy": 0, "avg_response_ms": 0.0, "times": []}
            s = services[r.service]
            s["total"] += 1
            if r.status == "healthy":
                s["healthy"] += 1
            s["times"].append(r.response_time_ms)
        result = {}
        for svc, data in services.items():
            avg = sum(data["times"]) / len(data["times"]) if data["times"] else 0
            result[svc] = {
                "total_checks": data["total"],
                "healthy_checks": data["healthy"],
                "uptime_pct": round(data["healthy"] / data["total"] * 100, 2) if data["total"] else 0,
                "avg_response_ms": round(avg, 2),
            }
        return result


store = MetricsStore()


@app.get("/health")
def health():
    logger.debug("Health check requested")
    return {"status": "healthy", "service": "analytics-api"}


@app.post("/metrics", status_code=201)
def post_metric(payload: MetricPayload):
    record = MetricRecord(
        service=payload.service,
        status=payload.status,
        response_time_ms=payload.response_time_ms,
        timestamp=payload.timestamp or time.time(),
    )
    store.add(record)
    return {"recorded": True, "service": record.service, "timestamp": record.timestamp}


@app.get("/metrics")
def get_metrics(service: str | None = None):
    if service:
        records = store.get_by_service(service)
    else:
        records = store.get_all()
    return {"count": len(records), "metrics": [r.__dict__ for r in records]}


@app.get("/metrics/summary")
def get_summary():
    return store.summary()


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("ANALYTICS_PORT", "8001"))
    logger.info("Starting Analytics API on port %d", port)
    uvicorn.run(app, host="0.0.0.0", port=port)
