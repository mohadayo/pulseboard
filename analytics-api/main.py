import logging
import math
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Literal

from fastapi import FastAPI, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field, ValidationError, field_validator

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("analytics-api")

app = FastAPI(title="PulseBoard Analytics API", version="1.0.0")

MAX_RECORDS = int(os.getenv("MAX_RECORDS", "10000"))
MAX_SERVICE_LENGTH = 100
MAX_RESPONSE_TIME_MS = 60_000.0
METRICS_DEFAULT_LIMIT = max(1, int(os.getenv("METRICS_DEFAULT_LIMIT", "100")))
METRICS_MAX_LIMIT = max(METRICS_DEFAULT_LIMIT, int(os.getenv("METRICS_MAX_LIMIT", "1000")))
BATCH_MAX_SIZE = max(1, int(os.getenv("METRICS_BATCH_MAX_SIZE", "500")))
ALLOWED_STATUSES = ("healthy", "unhealthy", "degraded", "unknown")
StatusLiteral = Literal["healthy", "unhealthy", "degraded", "unknown"]
SortFieldLiteral = Literal["timestamp", "service", "response_time_ms", "status"]
SortOrderLiteral = Literal["asc", "desc"]
ServiceSortFieldLiteral = Literal[
    "service", "total_checks", "last_seen", "first_seen", "latest_status"
]


def _percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = (pct / 100.0) * (len(sorted_values) - 1)
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return sorted_values[lower]
    weight = rank - lower
    return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight


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

    def filter(
        self,
        service: str | None = None,
        status: str | None = None,
        since: float | None = None,
        until: float | None = None,
    ) -> list[MetricRecord]:
        with self._lock:
            results = list(self.records)
        if service is not None:
            results = [r for r in results if r.service == service]
        if status is not None:
            results = [r for r in results if r.status == status]
        if since is not None:
            results = [r for r in results if r.timestamp >= since]
        if until is not None:
            results = [r for r in results if r.timestamp <= until]
        return results

    def delete_by_service(self, service: str) -> int:
        with self._lock:
            before = len(self.records)
            self.records = [r for r in self.records if r.service != service]
            deleted = before - len(self.records)
        if deleted > 0:
            logger.info("Deleted %d records for service=%s", deleted, service)
        return deleted

    def summary(
        self,
        service: str | None = None,
        status: str | None = None,
        since: float | None = None,
        until: float | None = None,
    ) -> dict:
        records_snapshot = self.filter(
            service=service, status=status, since=since, until=until,
        )
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
            times = data["times"]
            avg = sum(times) / len(times) if times else 0
            sorted_times = sorted(times)
            result[svc] = {
                "total_checks": data["total"],
                "healthy_checks": data["healthy"],
                "uptime_pct": round(data["healthy"] / data["total"] * 100, 2) if data["total"] else 0,
                "avg_response_ms": round(avg, 2),
                "min_response_ms": round(sorted_times[0], 2) if sorted_times else 0.0,
                "max_response_ms": round(sorted_times[-1], 2) if sorted_times else 0.0,
                "p50_response_ms": round(_percentile(sorted_times, 50), 2),
                "p95_response_ms": round(_percentile(sorted_times, 95), 2),
                "p99_response_ms": round(_percentile(sorted_times, 99), 2),
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


def _format_validation_error(exc: ValidationError) -> str:
    parts = []
    for err in exc.errors():
        loc = ".".join(str(p) for p in err.get("loc", ()) if p != "")
        msg = err.get("msg", "invalid")
        if loc:
            parts.append(f"{loc}: {msg}")
        else:
            parts.append(msg)
    return "; ".join(parts) if parts else "invalid payload"


@app.post("/metrics/batch")
async def post_metrics_batch(request: Request, response: Response):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Request body must be valid JSON")
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Request body must be a JSON object")
    metrics = body.get("metrics")
    if not isinstance(metrics, list):
        raise HTTPException(status_code=400, detail="Field 'metrics' must be an array")
    if len(metrics) == 0:
        raise HTTPException(status_code=400, detail="Field 'metrics' must not be empty")
    if len(metrics) > BATCH_MAX_SIZE:
        raise HTTPException(
            status_code=400,
            detail=f"Field 'metrics' must contain at most {BATCH_MAX_SIZE} items",
        )

    accepted: list[dict] = []
    rejected: list[dict] = []
    for index, item in enumerate(metrics):
        if not isinstance(item, dict):
            rejected.append({"index": index, "error": "item must be a JSON object"})
            continue
        try:
            payload = MetricPayload.model_validate(item)
        except ValidationError as e:
            rejected.append({"index": index, "error": _format_validation_error(e)})
            continue
        record = MetricRecord(
            service=payload.service,
            status=payload.status,
            response_time_ms=payload.response_time_ms,
            timestamp=payload.timestamp or time.time(),
        )
        store.add(record)
        accepted.append({
            "index": index,
            "service": record.service,
            "timestamp": record.timestamp,
        })

    total = len(metrics)
    if len(rejected) == total:
        status_code = 400
    elif rejected:
        status_code = 207  # Multi-Status: partial success
    else:
        status_code = 201

    logger.info(
        "Batch ingest: total=%d accepted=%d rejected=%d",
        total, len(accepted), len(rejected),
    )

    response.status_code = status_code
    return {
        "total": total,
        "accepted_count": len(accepted),
        "rejected_count": len(rejected),
        "accepted": accepted,
        "rejected": rejected,
    }


@app.get("/metrics")
def get_metrics(
    service: str | None = None,
    status: StatusLiteral | None = Query(
        default=None,
        description=f"ステータスで絞り込み（{', '.join(ALLOWED_STATUSES)}）",
    ),
    since: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以降（>=）のレコードに絞り込む",
    ),
    until: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以前（<=）のレコードに絞り込む",
    ),
    limit: int = Query(
        default=METRICS_DEFAULT_LIMIT,
        ge=1,
        le=METRICS_MAX_LIMIT,
        description=f"返却件数上限（最大 {METRICS_MAX_LIMIT}）",
    ),
    offset: int = Query(
        default=0,
        ge=0,
        description="先頭から読み飛ばす件数",
    ),
    sort: SortFieldLiteral = Query(
        default="timestamp",
        description="ソートフィールド（timestamp / service / response_time_ms / status）",
    ),
    order: SortOrderLiteral = Query(
        default="asc",
        description="ソート順（asc / desc）",
    ),
):
    if since is not None and until is not None and since > until:
        raise HTTPException(
            status_code=400,
            detail="since must be less than or equal to until",
        )
    if since is not None and not math.isfinite(since):
        raise HTTPException(status_code=400, detail="since must be a finite number")
    if until is not None and not math.isfinite(until):
        raise HTTPException(status_code=400, detail="until must be a finite number")

    records = store.filter(service=service, status=status, since=since, until=until)
    reverse = order == "desc"
    records = sorted(records, key=lambda r: getattr(r, sort), reverse=reverse)
    total = len(records)
    page = records[offset:offset + limit]
    return {
        "count": len(page),
        "total": total,
        "limit": limit,
        "offset": offset,
        "sort": sort,
        "order": order,
        "metrics": [r.__dict__ for r in page],
    }


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
def get_summary(
    service: str | None = None,
    status: StatusLiteral | None = Query(
        default=None,
        description=f"ステータスで絞り込み（{', '.join(ALLOWED_STATUSES)}）",
    ),
    since: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以降（>=）のレコードに絞り込む",
    ),
    until: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以前（<=）のレコードに絞り込む",
    ),
):
    if since is not None and until is not None and since > until:
        raise HTTPException(
            status_code=400,
            detail="since must be less than or equal to until",
        )
    if since is not None and not math.isfinite(since):
        raise HTTPException(status_code=400, detail="since must be a finite number")
    if until is not None and not math.isfinite(until):
        raise HTTPException(status_code=400, detail="until must be a finite number")
    return store.summary(service=service, status=status, since=since, until=until)


@app.get("/metrics/services")
def list_services(
    status: StatusLiteral | None = Query(
        default=None,
        description=f"最新ステータスで絞り込み（{', '.join(ALLOWED_STATUSES)}）",
    ),
    since: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以降（>=）の観測のみ集計",
    ),
    until: float | None = Query(
        default=None,
        ge=0,
        description="この Unix timestamp 以前（<=）の観測のみ集計",
    ),
    sort: ServiceSortFieldLiteral = Query(
        default="service",
        description=(
            "ソートフィールド（service / total_checks / last_seen / first_seen / latest_status）"
        ),
    ),
    order: SortOrderLiteral = Query(
        default="asc",
        description="ソート順（asc / desc）",
    ),
    limit: int = Query(
        default=METRICS_DEFAULT_LIMIT,
        ge=1,
        le=METRICS_MAX_LIMIT,
        description=f"返却件数上限（最大 {METRICS_MAX_LIMIT}）",
    ),
    offset: int = Query(
        default=0,
        ge=0,
        description="先頭から読み飛ばす件数",
    ),
):
    if since is not None and until is not None and since > until:
        raise HTTPException(
            status_code=400,
            detail="since must be less than or equal to until",
        )
    if since is not None and not math.isfinite(since):
        raise HTTPException(status_code=400, detail="since must be a finite number")
    if until is not None and not math.isfinite(until):
        raise HTTPException(status_code=400, detail="until must be a finite number")

    records = store.filter(since=since, until=until)
    by_service: dict[str, dict] = {}
    for r in records:
        existing = by_service.get(r.service)
        if existing is None:
            by_service[r.service] = {
                "service": r.service,
                "total_checks": 1,
                "first_seen": r.timestamp,
                "last_seen": r.timestamp,
                "latest_status": r.status,
            }
            continue
        existing["total_checks"] += 1
        if r.timestamp < existing["first_seen"]:
            existing["first_seen"] = r.timestamp
        if r.timestamp >= existing["last_seen"]:
            existing["last_seen"] = r.timestamp
            existing["latest_status"] = r.status

    services = list(by_service.values())
    if status is not None:
        services = [s for s in services if s["latest_status"] == status]

    reverse = order == "desc"
    services.sort(key=lambda s: s[sort], reverse=reverse)
    total = len(services)
    page = services[offset:offset + limit]
    return {
        "count": len(page),
        "total": total,
        "limit": limit,
        "offset": offset,
        "sort": sort,
        "order": order,
        "services": page,
    }


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("ANALYTICS_PORT", "8001"))
    logger.info("Starting Analytics API on port %d", port)
    uvicorn.run(app, host="0.0.0.0", port=port)
