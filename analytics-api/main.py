import logging
import math
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Literal

from fastapi import FastAPI, Query
from pydantic import BaseModel, Field, field_validator

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("analytics-api")

app = FastAPI(title="PulseBoard Analytics API", version="1.0.0")

MAX_RECORDS = int(os.getenv("MAX_RECORDS", "10000"))
MAX_SERVICE_LENGTH = 100
MAX_RESPONSE_TIME_MS = 60_000.0
ALLOWED_STATUSES = ("healthy", "unhealthy", "degraded", "unknown")
StatusLiteral = Literal["healthy", "unhealthy", "degraded", "unknown"]


class MetricPayload(BaseModel):
    service: str = Field(..., min_length=1, max_length=MAX_SERVICE_LENGTH)
    status: StatusLiteral
    response_time_ms: float = Field(..., ge=0, le=MAX_RESPONSE_TIME_MS)
    timestamp: float | None = Field(default=None, gt=0)

    @field_validator("service")
    @classmethod
    def validate_service(cls, v: str) -> str:
        stripped = v.strip()
        if not stripped:
            raise ValueError("service must not be blank")
        if len(stripped) > MAX_SERVICE_LENGTH:
            raise ValueError(f"service must be at most {MAX_SERVICE_LENGTH} characters")
        return stripped

    @field_validator("response_time_ms")
    @classmethod
    def validate_response_time(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("response_time_ms must be a finite number")
        return v

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: float | None) -> float | None:
        if v is None:
            return None
        if not math.isfinite(v):
            raise ValueError("timestamp must be a finite number")
        return v


@dataclass
class MetricRecord:
    service: str
    status: str
    response_time_ms: float
    timestamp: float


@dataclass
class MetricsStore:
    records: list[MetricRecord] = field(default_factory=list)
    max_records: int = MAX_RECORDS
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def add(self, record: MetricRecord) -> MetricRecord:
        with self._lock:
            self.records.append(record)
            if len(self.records) > self.max_records:
                removed = len(self.records) - self.max_records
                del self.records[:removed]
                logger.info("Evicted %d old records (store capped at %d)", removed, self.max_records)
        logger.info("Recorded metric for service=%s status=%s", record.service, record.status)
        return record

    def get_all(self) -> list[MetricRecord]:
        with self._lock:
            return list(self.records)

    def get_by_service(self, service: str) -> list[MetricRecord]:
        with self._lock:
            return [r for r in self.records if r.service == service]

    def delete_by_service(self, service: str) -> int:
        with self._lock:
            before = len(self.records)
            self.records = [r for r in self.records if r.service != service]
            deleted = before - len(self.records)
        if deleted > 0:
            logger.info("Deleted %d records for service=%s", deleted, service)
        return deleted

    def summary(self) -> dict:
        with self._lock:
            records_snapshot = list(self.records)
        services: dict[str, dict] = {}
        for r in records_snapshot:
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
    return {"status": "healthy", "service": "analytics-api", "timestamp": time.time()}


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


@app.delete("/metrics")
def delete_metrics(
    service: str = Query(
        ...,
        description="削除対象のサービス名",
        min_length=1,
        max_length=MAX_SERVICE_LENGTH,
    ),
):
    normalized = service.strip()
    if not normalized:
        return {"error": "service must not be blank", "deleted_count": 0}
    deleted = store.delete_by_service(normalized)
    if deleted == 0:
        return {"error": "No metrics found for the specified service", "deleted_count": 0}
    return {"message": "Metrics deleted", "service": normalized, "deleted_count": deleted}


@app.get("/metrics/summary")
def get_summary():
    return store.summary()


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("ANALYTICS_PORT", "8001"))
    logger.info("Starting Analytics API on port %d", port)
    uvicorn.run(app, host="0.0.0.0", port=port)
