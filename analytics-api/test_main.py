import time

from fastapi.testclient import TestClient

from main import MetricRecord, MetricsStore, app, store


client = TestClient(app)


def setup_function():
    store.records.clear()


def test_health():
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert data["service"] == "analytics-api"
    assert "timestamp" in data
    assert isinstance(data["timestamp"], float)


def test_post_metric():
    payload = {"service": "web", "status": "healthy", "response_time_ms": 42.5}
    resp = client.post("/metrics", json=payload)
    assert resp.status_code == 201
    data = resp.json()
    assert data["recorded"] is True
    assert data["service"] == "web"


def test_post_metric_negative_response_time():
    payload = {"service": "web", "status": "healthy", "response_time_ms": -10.0}
    resp = client.post("/metrics", json=payload)
    assert resp.status_code == 422


def test_post_metric_zero_response_time():
    payload = {"service": "web", "status": "healthy", "response_time_ms": 0.0}
    resp = client.post("/metrics", json=payload)
    assert resp.status_code == 201


def test_get_metrics_empty():
    resp = client.get("/metrics")
    assert resp.status_code == 200
    assert resp.json()["count"] == 0


def test_get_metrics_filtered():
    client.post("/metrics", json={"service": "api", "status": "healthy", "response_time_ms": 10})
    client.post("/metrics", json={"service": "db", "status": "unhealthy", "response_time_ms": 500})
    resp = client.get("/metrics?service=api")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 1
    assert data["metrics"][0]["service"] == "api"


def test_summary():
    client.post("/metrics", json={"service": "svc1", "status": "healthy", "response_time_ms": 20})
    client.post("/metrics", json={"service": "svc1", "status": "healthy", "response_time_ms": 30})
    client.post("/metrics", json={"service": "svc1", "status": "unhealthy", "response_time_ms": 100})
    resp = client.get("/metrics/summary")
    assert resp.status_code == 200
    data = resp.json()
    assert "svc1" in data
    assert data["svc1"]["total_checks"] == 3
    assert data["svc1"]["healthy_checks"] == 2
    assert data["svc1"]["uptime_pct"] == 66.67


def test_metrics_store_unit():
    s = MetricsStore()
    s.add(MetricRecord(
        service="x", status="healthy", response_time_ms=5, timestamp=time.time()
    ))
    assert len(s.get_all()) == 1
    assert len(s.get_by_service("x")) == 1
    assert len(s.get_by_service("y")) == 0


def test_metrics_store_max_capacity():
    s = MetricsStore(max_records=3)
    for i in range(5):
        s.add(MetricRecord(
            service=f"svc-{i}", status="healthy", response_time_ms=10.0, timestamp=time.time()
        ))
    assert len(s.get_all()) == 3
    services = [r.service for r in s.get_all()]
    assert "svc-0" not in services
    assert "svc-1" not in services
    assert "svc-4" in services


def test_metrics_store_eviction_preserves_order():
    s = MetricsStore(max_records=2)
    for i in range(4):
        s.add(MetricRecord(
            service=f"s{i}", status="healthy", response_time_ms=float(i), timestamp=time.time()
        ))
    records = s.get_all()
    assert len(records) == 2
    assert records[0].service == "s2"
    assert records[1].service == "s3"


def test_metrics_store_at_exact_capacity():
    s = MetricsStore(max_records=3)
    for i in range(3):
        s.add(MetricRecord(
            service=f"svc-{i}", status="healthy", response_time_ms=10.0, timestamp=time.time()
        ))
    assert len(s.get_all()) == 3
    services = [r.service for r in s.get_all()]
    assert services == ["svc-0", "svc-1", "svc-2"]


def test_post_metric_missing_required_fields():
    resp = client.post("/metrics", json={})
    assert resp.status_code == 422


def test_post_metric_explicit_timestamp():
    payload = {
        "service": "web",
        "status": "healthy",
        "response_time_ms": 10.0,
        "timestamp": 1700000000.0,
    }
    resp = client.post("/metrics", json=payload)
    assert resp.status_code == 201
    data = resp.json()
    assert data["timestamp"] == 1700000000.0


def test_summary_empty_store():
    resp = client.get("/metrics/summary")
    assert resp.status_code == 200
    assert resp.json() == {}


def test_get_metrics_unknown_service():
    resp = client.get("/metrics?service=nonexistent")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 0
    assert data["metrics"] == []


def test_delete_metrics_success():
    client.post("/metrics", json={"service": "to_delete", "status": "healthy", "response_time_ms": 10})
    client.post("/metrics", json={"service": "to_delete", "status": "unhealthy", "response_time_ms": 20})
    client.post("/metrics", json={"service": "keep", "status": "healthy", "response_time_ms": 30})

    resp = client.delete("/metrics?service=to_delete")
    assert resp.status_code == 200
    data = resp.json()
    assert data["message"] == "Metrics deleted"
    assert data["deleted_count"] == 2
    assert data["service"] == "to_delete"

    remaining = client.get("/metrics").json()
    assert remaining["count"] == 1
    assert remaining["metrics"][0]["service"] == "keep"


def test_delete_metrics_not_found():
    resp = client.delete("/metrics?service=nonexistent")
    assert resp.status_code == 200
    data = resp.json()
    assert data["deleted_count"] == 0


def test_delete_metrics_missing_param():
    resp = client.delete("/metrics")
    assert resp.status_code == 422


def test_delete_metrics_updates_summary():
    client.post("/metrics", json={"service": "svc_a", "status": "healthy", "response_time_ms": 10})
    client.post("/metrics", json={"service": "svc_b", "status": "healthy", "response_time_ms": 20})

    client.delete("/metrics?service=svc_a")

    summary = client.get("/metrics/summary").json()
    assert "svc_a" not in summary
    assert "svc_b" in summary


def test_metrics_store_delete_by_service_unit():
    s = MetricsStore()
    s.add(MetricRecord(service="a", status="healthy", response_time_ms=10, timestamp=time.time()))
    s.add(MetricRecord(service="b", status="healthy", response_time_ms=20, timestamp=time.time()))
    s.add(MetricRecord(service="a", status="unhealthy", response_time_ms=30, timestamp=time.time()))

    deleted = s.delete_by_service("a")
    assert deleted == 2
    assert len(s.get_all()) == 1
    assert s.get_all()[0].service == "b"


def test_metrics_store_delete_nonexistent():
    s = MetricsStore()
    s.add(MetricRecord(service="x", status="healthy", response_time_ms=5, timestamp=time.time()))
    deleted = s.delete_by_service("y")
    assert deleted == 0
    assert len(s.get_all()) == 1


def test_post_metric_rejects_invalid_status():
    payload = {"service": "web", "status": "broken", "response_time_ms": 10.0}
    resp = client.post("/metrics", json=payload)
    assert resp.status_code == 422


def test_post_metric_rejects_blank_service():
    payload = {"service": "   ", "status": "healthy", "response_time_ms": 10.0}
    resp = client.post("/metrics", json=payload)
    assert resp.status_code == 422


def test_post_metric_rejects_empty_service():
    payload = {"service": "", "status": "healthy", "response_time_ms": 10.0}
    resp = client.post("/metrics", json=payload)
    assert resp.status_code == 422


def test_post_metric_rejects_overlong_service():
    payload = {"service": "x" * 101, "status": "healthy", "response_time_ms": 10.0}
    resp = client.post("/metrics", json=payload)
    assert resp.status_code == 422


def test_post_metric_accepts_max_length_service():
    payload = {"service": "x" * 100, "status": "healthy", "response_time_ms": 10.0}
    resp = client.post("/metrics", json=payload)
    assert resp.status_code == 201


def test_post_metric_rejects_excessive_response_time():
    payload = {"service": "web", "status": "healthy", "response_time_ms": 60001.0}
    resp = client.post("/metrics", json=payload)
    assert resp.status_code == 422


def test_post_metric_accepts_response_time_at_limit():
    payload = {"service": "web", "status": "healthy", "response_time_ms": 60000.0}
    resp = client.post("/metrics", json=payload)
    assert resp.status_code == 201


def test_post_metric_rejects_negative_timestamp():
    payload = {
        "service": "web",
        "status": "healthy",
        "response_time_ms": 10.0,
        "timestamp": -1.0,
    }
    resp = client.post("/metrics", json=payload)
    assert resp.status_code == 422


def test_post_metric_accepts_all_allowed_statuses():
    for status in ("healthy", "unhealthy", "degraded", "unknown"):
        resp = client.post(
            "/metrics",
            json={"service": "web", "status": status, "response_time_ms": 1.0},
        )
        assert resp.status_code == 201, f"status={status} failed"


def test_post_metric_strips_whitespace_in_service():
    payload = {"service": "  web  ", "status": "healthy", "response_time_ms": 10.0}
    resp = client.post("/metrics", json=payload)
    assert resp.status_code == 201
    assert resp.json()["service"] == "web"


def test_delete_metrics_rejects_overlong_service():
    resp = client.delete("/metrics?service=" + "x" * 101)
    assert resp.status_code == 422


def test_delete_metrics_rejects_empty_service():
    resp = client.delete("/metrics?service=")
    assert resp.status_code == 422
