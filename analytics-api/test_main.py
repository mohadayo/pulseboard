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
