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
    data = resp.json()
    assert data["count"] == 0
    assert data["total"] == 0
    assert data["offset"] == 0
    assert data["metrics"] == []


def test_get_metrics_filtered():
    client.post("/metrics", json={"service": "api", "status": "healthy", "response_time_ms": 10})
    client.post("/metrics", json={"service": "db", "status": "unhealthy", "response_time_ms": 500})
    resp = client.get("/metrics?service=api")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 1
    assert data["total"] == 1
    assert data["metrics"][0]["service"] == "api"


def test_get_metrics_pagination_basic():
    for i in range(5):
        client.post(
            "/metrics",
            json={"service": f"svc{i}", "status": "healthy", "response_time_ms": float(i)},
        )
    resp = client.get("/metrics?limit=2&offset=1")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 5
    assert data["count"] == 2
    assert data["limit"] == 2
    assert data["offset"] == 1
    services = [m["service"] for m in data["metrics"]]
    assert services == ["svc1", "svc2"]


def test_get_metrics_offset_beyond_total_returns_empty():
    client.post("/metrics", json={"service": "only", "status": "healthy", "response_time_ms": 1})
    resp = client.get("/metrics?offset=999")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert data["count"] == 0
    assert data["metrics"] == []


def test_get_metrics_rejects_negative_limit():
    resp = client.get("/metrics?limit=-1")
    assert resp.status_code == 422


def test_get_metrics_rejects_zero_limit():
    resp = client.get("/metrics?limit=0")
    assert resp.status_code == 422


def test_get_metrics_rejects_limit_above_max():
    resp = client.get("/metrics?limit=99999")
    assert resp.status_code == 422


def test_get_metrics_rejects_negative_offset():
    resp = client.get("/metrics?offset=-1")
    assert resp.status_code == 422


def test_get_metrics_filter_then_paginate():
    for i in range(4):
        client.post(
            "/metrics",
            json={"service": "target", "status": "healthy", "response_time_ms": float(i)},
        )
    client.post("/metrics", json={"service": "other", "status": "healthy", "response_time_ms": 99})
    resp = client.get("/metrics?service=target&limit=2&offset=1")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 4
    assert data["count"] == 2
    services = {m["service"] for m in data["metrics"]}
    assert services == {"target"}


def test_get_metrics_filter_since():
    client.post("/metrics", json={"service": "old", "status": "healthy", "response_time_ms": 1, "timestamp": 1000.0})
    client.post("/metrics", json={"service": "mid", "status": "healthy", "response_time_ms": 1, "timestamp": 2000.0})
    client.post("/metrics", json={"service": "new", "status": "healthy", "response_time_ms": 1, "timestamp": 3000.0})
    resp = client.get("/metrics?since=2000")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 2
    services = {m["service"] for m in data["metrics"]}
    assert services == {"mid", "new"}


def test_get_metrics_filter_until():
    client.post("/metrics", json={"service": "old", "status": "healthy", "response_time_ms": 1, "timestamp": 1000.0})
    client.post("/metrics", json={"service": "mid", "status": "healthy", "response_time_ms": 1, "timestamp": 2000.0})
    client.post("/metrics", json={"service": "new", "status": "healthy", "response_time_ms": 1, "timestamp": 3000.0})
    resp = client.get("/metrics?until=2000")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 2
    services = {m["service"] for m in data["metrics"]}
    assert services == {"old", "mid"}


def test_get_metrics_filter_since_and_until():
    client.post("/metrics", json={"service": "a", "status": "healthy", "response_time_ms": 1, "timestamp": 1000.0})
    client.post("/metrics", json={"service": "b", "status": "healthy", "response_time_ms": 1, "timestamp": 2000.0})
    client.post("/metrics", json={"service": "c", "status": "healthy", "response_time_ms": 1, "timestamp": 3000.0})
    client.post("/metrics", json={"service": "d", "status": "healthy", "response_time_ms": 1, "timestamp": 4000.0})
    resp = client.get("/metrics?since=1500&until=3500")
    assert resp.status_code == 200
    data = resp.json()
    services = {m["service"] for m in data["metrics"]}
    assert services == {"b", "c"}


def test_get_metrics_filter_since_combined_with_service():
    client.post("/metrics", json={"service": "x", "status": "healthy", "response_time_ms": 1, "timestamp": 1000.0})
    client.post("/metrics", json={"service": "x", "status": "healthy", "response_time_ms": 1, "timestamp": 5000.0})
    client.post("/metrics", json={"service": "y", "status": "healthy", "response_time_ms": 1, "timestamp": 5000.0})
    resp = client.get("/metrics?service=x&since=2000")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert data["metrics"][0]["service"] == "x"
    assert data["metrics"][0]["timestamp"] == 5000.0


def test_get_metrics_rejects_since_greater_than_until():
    resp = client.get("/metrics?since=3000&until=1000")
    assert resp.status_code == 400
    assert "since" in resp.json()["detail"].lower()


def test_get_metrics_rejects_negative_since():
    resp = client.get("/metrics?since=-1")
    assert resp.status_code == 422


def test_get_metrics_rejects_negative_until():
    resp = client.get("/metrics?until=-1")
    assert resp.status_code == 422


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


def test_summary_filter_by_service():
    client.post("/metrics", json={"service": "svc-a", "status": "healthy", "response_time_ms": 10})
    client.post("/metrics", json={"service": "svc-a", "status": "healthy", "response_time_ms": 20})
    client.post("/metrics", json={"service": "svc-b", "status": "unhealthy", "response_time_ms": 30})
    resp = client.get("/metrics/summary?service=svc-a")
    assert resp.status_code == 200
    data = resp.json()
    assert list(data.keys()) == ["svc-a"]
    assert data["svc-a"]["total_checks"] == 2


def test_summary_filter_by_status():
    client.post("/metrics", json={"service": "svc-a", "status": "healthy", "response_time_ms": 10})
    client.post("/metrics", json={"service": "svc-a", "status": "unhealthy", "response_time_ms": 50})
    resp = client.get("/metrics/summary?status=unhealthy")
    assert resp.status_code == 200
    data = resp.json()
    assert data["svc-a"]["total_checks"] == 1
    assert data["svc-a"]["healthy_checks"] == 0
    assert data["svc-a"]["uptime_pct"] == 0


def test_summary_filter_by_time_range():
    client.post(
        "/metrics",
        json={"service": "svc-a", "status": "healthy", "response_time_ms": 10, "timestamp": 100.0},
    )
    client.post(
        "/metrics",
        json={"service": "svc-a", "status": "healthy", "response_time_ms": 20, "timestamp": 200.0},
    )
    client.post(
        "/metrics",
        json={"service": "svc-a", "status": "healthy", "response_time_ms": 30, "timestamp": 300.0},
    )
    resp = client.get("/metrics/summary?since=150&until=250")
    assert resp.status_code == 200
    data = resp.json()
    assert data["svc-a"]["total_checks"] == 1


def test_summary_invalid_range():
    resp = client.get("/metrics/summary?since=200&until=100")
    assert resp.status_code == 400


def test_summary_invalid_status():
    resp = client.get("/metrics/summary?status=bogus")
    assert resp.status_code == 422


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


def test_get_metrics_filters_by_status():
    client.post("/metrics", json={"service": "a", "status": "healthy", "response_time_ms": 1})
    client.post("/metrics", json={"service": "b", "status": "unhealthy", "response_time_ms": 2})
    client.post("/metrics", json={"service": "c", "status": "degraded", "response_time_ms": 3})
    resp = client.get("/metrics?status=unhealthy")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert data["metrics"][0]["service"] == "b"
    assert data["metrics"][0]["status"] == "unhealthy"


def test_get_metrics_status_combined_with_service():
    client.post("/metrics", json={"service": "web", "status": "healthy", "response_time_ms": 1})
    client.post("/metrics", json={"service": "web", "status": "unhealthy", "response_time_ms": 2})
    client.post("/metrics", json={"service": "db", "status": "unhealthy", "response_time_ms": 3})
    resp = client.get("/metrics?service=web&status=unhealthy")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert data["metrics"][0]["service"] == "web"
    assert data["metrics"][0]["status"] == "unhealthy"


def test_get_metrics_rejects_invalid_status():
    resp = client.get("/metrics?status=bogus")
    assert resp.status_code == 422


def test_metrics_store_filter_status_directly():
    s = MetricsStore()
    s.add(MetricRecord("svc", "healthy", 1.0, time.time()))
    s.add(MetricRecord("svc", "degraded", 2.0, time.time()))
    assert len(s.filter(status="degraded")) == 1
    assert len(s.filter(status="healthy")) == 1
    assert len(s.filter(status="unknown")) == 0


def test_summary_includes_min_max_and_percentiles():
    for i in range(1, 11):
        client.post(
            "/metrics",
            json={"service": "svc-p", "status": "healthy", "response_time_ms": float(i * 10)},
        )
    resp = client.get("/metrics/summary")
    assert resp.status_code == 200
    data = resp.json()["svc-p"]
    assert data["min_response_ms"] == 10.0
    assert data["max_response_ms"] == 100.0
    assert data["p50_response_ms"] == 55.0
    assert data["p95_response_ms"] >= 90.0
    assert data["p99_response_ms"] >= 95.0


def test_summary_percentile_single_sample():
    client.post("/metrics", json={"service": "single", "status": "healthy", "response_time_ms": 42.0})
    resp = client.get("/metrics/summary")
    data = resp.json()["single"]
    assert data["min_response_ms"] == 42.0
    assert data["max_response_ms"] == 42.0
    assert data["p50_response_ms"] == 42.0
    assert data["p95_response_ms"] == 42.0
    assert data["p99_response_ms"] == 42.0


def test_get_metrics_sort_by_response_time_asc():
    for v in [50.0, 10.0, 30.0]:
        client.post("/metrics", json={"service": "svc", "status": "healthy", "response_time_ms": v})
    resp = client.get("/metrics?sort=response_time_ms&order=asc")
    assert resp.status_code == 200
    data = resp.json()
    assert data["sort"] == "response_time_ms"
    assert data["order"] == "asc"
    rts = [m["response_time_ms"] for m in data["metrics"]]
    assert rts == [10.0, 30.0, 50.0]


def test_get_metrics_sort_by_response_time_desc():
    for v in [50.0, 10.0, 30.0]:
        client.post("/metrics", json={"service": "svc", "status": "healthy", "response_time_ms": v})
    resp = client.get("/metrics?sort=response_time_ms&order=desc")
    assert resp.status_code == 200
    rts = [m["response_time_ms"] for m in resp.json()["metrics"]]
    assert rts == [50.0, 30.0, 10.0]


def test_get_metrics_sort_by_service_alpha():
    client.post("/metrics", json={"service": "zebra", "status": "healthy", "response_time_ms": 1.0})
    client.post("/metrics", json={"service": "apple", "status": "healthy", "response_time_ms": 2.0})
    client.post("/metrics", json={"service": "mango", "status": "healthy", "response_time_ms": 3.0})
    resp = client.get("/metrics?sort=service")
    services = [m["service"] for m in resp.json()["metrics"]]
    assert services == ["apple", "mango", "zebra"]


def test_get_metrics_rejects_invalid_sort_field():
    resp = client.get("/metrics?sort=bogus")
    assert resp.status_code == 422


def test_get_metrics_rejects_invalid_sort_order():
    resp = client.get("/metrics?order=sideways")
    assert resp.status_code == 422


def test_list_services_empty():
    resp = client.get("/metrics/services")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 0
    assert data["services"] == []
    assert data["sort"] == "service"


def test_list_services_distinct_aggregation():
    client.post("/metrics", json={
        "service": "web", "status": "healthy", "response_time_ms": 1.0, "timestamp": 100.0,
    })
    client.post("/metrics", json={
        "service": "web", "status": "unhealthy", "response_time_ms": 2.0, "timestamp": 200.0,
    })
    client.post("/metrics", json={
        "service": "db", "status": "healthy", "response_time_ms": 3.0, "timestamp": 150.0,
    })
    resp = client.get("/metrics/services")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 2
    by_name = {s["service"]: s for s in data["services"]}
    assert by_name["web"]["total_checks"] == 2
    assert by_name["web"]["first_seen"] == 100.0
    assert by_name["web"]["last_seen"] == 200.0
    assert by_name["web"]["latest_status"] == "unhealthy"
    assert by_name["db"]["total_checks"] == 1
    assert by_name["db"]["latest_status"] == "healthy"


def test_list_services_default_sorted_by_service_asc():
    client.post("/metrics", json={"service": "zebra", "status": "healthy", "response_time_ms": 1.0})
    client.post("/metrics", json={"service": "apple", "status": "healthy", "response_time_ms": 1.0})
    client.post("/metrics", json={"service": "mango", "status": "healthy", "response_time_ms": 1.0})
    resp = client.get("/metrics/services")
    names = [s["service"] for s in resp.json()["services"]]
    assert names == ["apple", "mango", "zebra"]


def test_list_services_sort_by_total_checks_desc():
    for _ in range(3):
        client.post("/metrics", json={
            "service": "busy", "status": "healthy", "response_time_ms": 1.0,
        })
    client.post("/metrics", json={
        "service": "quiet", "status": "healthy", "response_time_ms": 1.0,
    })
    resp = client.get("/metrics/services?sort=total_checks&order=desc")
    names = [s["service"] for s in resp.json()["services"]]
    assert names == ["busy", "quiet"]


def test_list_services_sort_by_last_seen_desc():
    client.post("/metrics", json={
        "service": "old", "status": "healthy", "response_time_ms": 1.0, "timestamp": 100.0,
    })
    client.post("/metrics", json={
        "service": "new", "status": "healthy", "response_time_ms": 1.0, "timestamp": 200.0,
    })
    resp = client.get("/metrics/services?sort=last_seen&order=desc")
    names = [s["service"] for s in resp.json()["services"]]
    assert names == ["new", "old"]


def test_list_services_filter_by_status():
    client.post("/metrics", json={
        "service": "down", "status": "unhealthy", "response_time_ms": 1.0, "timestamp": 100.0,
    })
    client.post("/metrics", json={
        "service": "ok", "status": "healthy", "response_time_ms": 1.0, "timestamp": 100.0,
    })
    resp = client.get("/metrics/services?status=unhealthy")
    data = resp.json()
    assert data["total"] == 1
    assert data["services"][0]["service"] == "down"


def test_list_services_time_range_filter():
    client.post("/metrics", json={
        "service": "old", "status": "healthy", "response_time_ms": 1.0, "timestamp": 100.0,
    })
    client.post("/metrics", json={
        "service": "new", "status": "healthy", "response_time_ms": 1.0, "timestamp": 500.0,
    })
    resp = client.get("/metrics/services?since=400")
    data = resp.json()
    assert data["total"] == 1
    assert data["services"][0]["service"] == "new"


def test_list_services_pagination():
    for name in ["a", "b", "c", "d", "e"]:
        client.post("/metrics", json={
            "service": name, "status": "healthy", "response_time_ms": 1.0,
        })
    resp = client.get("/metrics/services?limit=2&offset=1")
    data = resp.json()
    assert data["count"] == 2
    assert data["total"] == 5
    assert [s["service"] for s in data["services"]] == ["b", "c"]


def test_list_services_rejects_invalid_sort():
    resp = client.get("/metrics/services?sort=bogus")
    assert resp.status_code == 422


def test_list_services_rejects_until_before_since():
    resp = client.get("/metrics/services?since=200&until=100")
    assert resp.status_code == 400


def test_list_services_latest_status_uses_most_recent_timestamp():
    # Out-of-order arrival: later timestamp should win even if posted first
    client.post("/metrics", json={
        "service": "svc", "status": "healthy", "response_time_ms": 1.0, "timestamp": 200.0,
    })
    client.post("/metrics", json={
        "service": "svc", "status": "unhealthy", "response_time_ms": 1.0, "timestamp": 100.0,
    })
    resp = client.get("/metrics/services")
    data = resp.json()
    assert data["services"][0]["latest_status"] == "healthy"
    assert data["services"][0]["last_seen"] == 200.0
    assert data["services"][0]["first_seen"] == 100.0
