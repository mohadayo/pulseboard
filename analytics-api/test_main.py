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


def test_summary_filter_by_q_partial_match():
    client.post("/metrics", json={"service": "payments-api", "status": "healthy", "response_time_ms": 10})
    client.post("/metrics", json={"service": "payments-worker", "status": "healthy", "response_time_ms": 20})
    client.post("/metrics", json={"service": "orders-api", "status": "unhealthy", "response_time_ms": 30})
    resp = client.get("/metrics/summary?q=payments")
    assert resp.status_code == 200
    data = resp.json()
    assert set(data.keys()) == {"payments-api", "payments-worker"}


def test_summary_q_case_insensitive():
    client.post("/metrics", json={"service": "Payments-API", "status": "healthy", "response_time_ms": 10})
    resp = client.get("/metrics/summary?q=PAYMENTS")
    assert resp.status_code == 200
    assert "Payments-API" in resp.json()


def test_summary_q_blank_rejected():
    # 既存 /metrics と挙動を揃え、trim 後が空の q は 400 を返す。
    resp = client.get("/metrics/summary?q=%20%20")
    assert resp.status_code == 400
    assert "blank" in resp.json()["detail"]


def test_summary_q_too_long_rejected():
    resp = client.get("/metrics/summary?q=" + "x" * 9999)
    assert resp.status_code == 400


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
    # service と before のどちらも未指定の場合は 400 で拒否される
    resp = client.delete("/metrics")
    assert resp.status_code == 400
    assert "service" in resp.json()["detail"]
    assert "before" in resp.json()["detail"]


def test_delete_metrics_by_before_only():
    # before のみ指定で古いレコードだけ削除されることを確認
    base = time.time()
    client.post("/metrics", json={
        "service": "old", "status": "healthy",
        "response_time_ms": 10, "timestamp": base - 1000,
    })
    client.post("/metrics", json={
        "service": "mid", "status": "healthy",
        "response_time_ms": 20, "timestamp": base - 500,
    })
    client.post("/metrics", json={
        "service": "new", "status": "healthy",
        "response_time_ms": 30, "timestamp": base,
    })

    cutoff = base - 400
    resp = client.delete(f"/metrics?before={cutoff}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["deleted_count"] == 2  # old と mid が削除される
    assert data["service"] is None
    assert data["before"] == cutoff

    remaining = client.get("/metrics").json()
    remaining_services = sorted(m["service"] for m in remaining["metrics"])
    assert remaining_services == ["new"]


def test_delete_metrics_by_service_and_before_combined():
    # service と before の AND 条件で削除されることを確認
    base = time.time()
    client.post("/metrics", json={
        "service": "web", "status": "healthy",
        "response_time_ms": 10, "timestamp": base - 1000,
    })
    client.post("/metrics", json={
        "service": "web", "status": "healthy",
        "response_time_ms": 20, "timestamp": base,
    })
    client.post("/metrics", json={
        "service": "db", "status": "healthy",
        "response_time_ms": 30, "timestamp": base - 1000,
    })

    cutoff = base - 500
    resp = client.delete(f"/metrics?service=web&before={cutoff}")
    assert resp.status_code == 200
    data = resp.json()
    # web で base - 1000 のレコード 1 件だけ削除される
    assert data["deleted_count"] == 1
    assert data["service"] == "web"
    assert data["before"] == cutoff

    remaining = client.get("/metrics").json()
    remaining_pairs = sorted(
        (m["service"], int(m["timestamp"])) for m in remaining["metrics"]
    )
    assert remaining_pairs == [
        ("db", int(base - 1000)),
        ("web", int(base)),
    ]


def test_delete_metrics_by_before_rejects_non_positive():
    # before は gt=0 でバリデーションされ 0 以下は 422
    for value in ("0", "-1"):
        resp = client.delete(f"/metrics?before={value}")
        assert resp.status_code == 422, value


def test_delete_metrics_before_no_match_returns_zero():
    client.post("/metrics", json={
        "service": "svc", "status": "healthy",
        "response_time_ms": 1, "timestamp": time.time(),
    })
    resp = client.delete("/metrics?before=1")
    assert resp.status_code == 200
    data = resp.json()
    assert data["deleted_count"] == 0
    assert "error" in data


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


def test_metrics_store_delete_unit():
    # service のみ
    s = MetricsStore()
    s.add(MetricRecord(service="a", status="healthy", response_time_ms=10, timestamp=100))
    s.add(MetricRecord(service="b", status="healthy", response_time_ms=20, timestamp=100))
    assert s.delete(service="a") == 1
    assert [r.service for r in s.get_all()] == ["b"]

    # before のみ
    s = MetricsStore()
    s.add(MetricRecord(service="a", status="healthy", response_time_ms=10, timestamp=100))
    s.add(MetricRecord(service="a", status="healthy", response_time_ms=20, timestamp=200))
    s.add(MetricRecord(service="b", status="healthy", response_time_ms=30, timestamp=50))
    assert s.delete(before=150) == 2  # timestamp 100 と 50 が削除
    remaining = sorted((r.service, r.timestamp) for r in s.get_all())
    assert remaining == [("a", 200)]

    # service + before の AND
    s = MetricsStore()
    s.add(MetricRecord(service="a", status="healthy", response_time_ms=10, timestamp=100))
    s.add(MetricRecord(service="a", status="healthy", response_time_ms=20, timestamp=300))
    s.add(MetricRecord(service="b", status="healthy", response_time_ms=30, timestamp=100))
    assert s.delete(service="a", before=200) == 1  # a で timestamp 100 のみ
    remaining = sorted((r.service, r.timestamp) for r in s.get_all())
    assert remaining == [("a", 300), ("b", 100)]

    # service=None かつ before=None は何もしない
    s = MetricsStore()
    s.add(MetricRecord(service="a", status="healthy", response_time_ms=10, timestamp=100))
    assert s.delete() == 0
    assert len(s.get_all()) == 1

    # before の境界: timestamp == before のレコードは「削除しない」(strict <)
    s = MetricsStore()
    s.add(MetricRecord(service="a", status="healthy", response_time_ms=10, timestamp=100))
    s.add(MetricRecord(service="a", status="healthy", response_time_ms=20, timestamp=99.999))
    assert s.delete(before=100) == 1
    remaining = [r.timestamp for r in s.get_all()]
    assert remaining == [100]


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


def test_list_services_filter_by_service():
    # service 指定時は該当サービスのみ集計される
    client.post("/metrics", json={
        "service": "web", "status": "healthy", "response_time_ms": 1.0, "timestamp": 100.0,
    })
    client.post("/metrics", json={
        "service": "web", "status": "unhealthy", "response_time_ms": 2.0, "timestamp": 200.0,
    })
    client.post("/metrics", json={
        "service": "db", "status": "healthy", "response_time_ms": 3.0, "timestamp": 150.0,
    })
    resp = client.get("/metrics/services?service=web")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert data["services"][0]["service"] == "web"
    assert data["services"][0]["total_checks"] == 2


def test_list_services_filter_by_service_combined_with_status():
    # service と status の両方を指定（AND 条件）
    client.post("/metrics", json={
        "service": "web", "status": "unhealthy", "response_time_ms": 1.0, "timestamp": 100.0,
    })
    client.post("/metrics", json={
        "service": "db", "status": "unhealthy", "response_time_ms": 1.0, "timestamp": 100.0,
    })
    resp = client.get("/metrics/services?service=web&status=unhealthy")
    data = resp.json()
    assert data["total"] == 1
    assert data["services"][0]["service"] == "web"

    # status が一致しなければ空になる
    resp = client.get("/metrics/services?service=web&status=healthy")
    assert resp.json()["total"] == 0


def test_list_services_filter_by_service_strips_whitespace():
    # service は POST 時に strip 保存されるためクエリ側も strip して照合する
    client.post("/metrics", json={
        "service": "web", "status": "healthy", "response_time_ms": 1.0, "timestamp": 100.0,
    })
    resp = client.get("/metrics/services?service=%20web%20")
    data = resp.json()
    assert data["total"] == 1
    assert data["services"][0]["service"] == "web"


def test_list_services_filter_by_service_unknown_returns_empty():
    client.post("/metrics", json={
        "service": "web", "status": "healthy", "response_time_ms": 1.0, "timestamp": 100.0,
    })
    resp = client.get("/metrics/services?service=nonexistent")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 0
    assert data["services"] == []


def test_list_services_blank_service_ignored_as_no_filter():
    # 空白のみの service はフィルタなし扱いとなり全サービスを集計する
    client.post("/metrics", json={
        "service": "web", "status": "healthy", "response_time_ms": 1.0, "timestamp": 100.0,
    })
    client.post("/metrics", json={
        "service": "db", "status": "healthy", "response_time_ms": 1.0, "timestamp": 100.0,
    })
    resp = client.get("/metrics/services?service=%20%20%20")
    assert resp.status_code == 200
    assert resp.json()["total"] == 2


def test_list_services_rejects_empty_service():
    resp = client.get("/metrics/services?service=")
    assert resp.status_code == 422


def test_list_services_rejects_overlong_service():
    resp = client.get("/metrics/services?service=" + "x" * 101)
    assert resp.status_code == 422


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


def test_list_services_includes_uptime_stats():
    # api: healthy=3 / total=4 → uptime 75.0
    # db : healthy=1 / total=2 → uptime 50.0
    for ts, status in [(1.0, "healthy"), (2.0, "healthy"), (3.0, "unhealthy"), (4.0, "healthy")]:
        client.post("/metrics", json={
            "service": "api", "status": status,
            "response_time_ms": 10.0, "timestamp": ts,
        })
    for ts, status in [(1.0, "healthy"), (2.0, "unhealthy")]:
        client.post("/metrics", json={
            "service": "db", "status": status,
            "response_time_ms": 10.0, "timestamp": ts,
        })

    resp = client.get("/metrics/services")
    assert resp.status_code == 200
    services = {s["service"]: s for s in resp.json()["services"]}

    assert services["api"]["total_checks"] == 4
    assert services["api"]["healthy_checks"] == 3
    assert services["api"]["uptime_pct"] == 75.0

    assert services["db"]["total_checks"] == 2
    assert services["db"]["healthy_checks"] == 1
    assert services["db"]["uptime_pct"] == 50.0


def test_list_services_uptime_pct_zero_when_no_healthy():
    client.post("/metrics", json={
        "service": "broken", "status": "unhealthy", "response_time_ms": 1.0, "timestamp": 1.0,
    })
    resp = client.get("/metrics/services")
    svc = resp.json()["services"][0]
    assert svc["healthy_checks"] == 0
    assert svc["uptime_pct"] == 0


def test_list_services_sort_by_uptime_pct_asc():
    # low (25%), mid (50%), high (100%)
    for ts, status in [(1.0, "healthy"), (2.0, "unhealthy"), (3.0, "unhealthy"), (4.0, "unhealthy")]:
        client.post("/metrics", json={
            "service": "low", "status": status, "response_time_ms": 1.0, "timestamp": ts,
        })
    for ts, status in [(1.0, "healthy"), (2.0, "unhealthy")]:
        client.post("/metrics", json={
            "service": "mid", "status": status, "response_time_ms": 1.0, "timestamp": ts,
        })
    for ts in [1.0, 2.0]:
        client.post("/metrics", json={
            "service": "high", "status": "healthy", "response_time_ms": 1.0, "timestamp": ts,
        })

    resp = client.get("/metrics/services?sort=uptime_pct&order=asc")
    assert resp.status_code == 200
    names = [s["service"] for s in resp.json()["services"]]
    assert names == ["low", "mid", "high"]


def test_list_services_sort_by_healthy_checks_desc():
    for ts in [1.0, 2.0, 3.0]:
        client.post("/metrics", json={
            "service": "a", "status": "healthy", "response_time_ms": 1.0, "timestamp": ts,
        })
    client.post("/metrics", json={
        "service": "b", "status": "healthy", "response_time_ms": 1.0, "timestamp": 1.0,
    })

    resp = client.get("/metrics/services?sort=healthy_checks&order=desc")
    names = [s["service"] for s in resp.json()["services"]]
    assert names == ["a", "b"]


def test_post_metrics_batch_all_valid():
    payload = {
        "metrics": [
            {"service": "web", "status": "healthy", "response_time_ms": 10.0},
            {"service": "db", "status": "unhealthy", "response_time_ms": 200.0},
            {"service": "cache", "status": "healthy", "response_time_ms": 5.0},
        ],
    }
    resp = client.post("/metrics/batch", json=payload)
    assert resp.status_code == 201
    data = resp.json()
    assert data["total"] == 3
    assert data["accepted_count"] == 3
    assert data["rejected_count"] == 0
    assert len(data["accepted"]) == 3
    assert data["rejected"] == []
    # Records actually persisted
    list_resp = client.get("/metrics")
    assert list_resp.json()["total"] == 3


def test_post_metrics_batch_partial_success_returns_207():
    payload = {
        "metrics": [
            {"service": "web", "status": "healthy", "response_time_ms": 10.0},
            {"service": "", "status": "healthy", "response_time_ms": 5.0},  # invalid
            {"service": "db", "status": "bogus", "response_time_ms": 1.0},  # invalid
        ],
    }
    resp = client.post("/metrics/batch", json=payload)
    assert resp.status_code == 207
    data = resp.json()
    assert data["total"] == 3
    assert data["accepted_count"] == 1
    assert data["rejected_count"] == 2
    rejected_indexes = [r["index"] for r in data["rejected"]]
    assert rejected_indexes == [1, 2]
    list_resp = client.get("/metrics")
    assert list_resp.json()["total"] == 1


def test_post_metrics_batch_all_invalid_returns_400():
    payload = {
        "metrics": [
            {"service": "", "status": "healthy", "response_time_ms": 10.0},
            {"service": "db", "status": "wrong", "response_time_ms": 1.0},
        ],
    }
    resp = client.post("/metrics/batch", json=payload)
    assert resp.status_code == 400
    data = resp.json()
    assert data["total"] == 2
    assert data["accepted_count"] == 0
    assert data["rejected_count"] == 2
    list_resp = client.get("/metrics")
    assert list_resp.json()["total"] == 0


def test_post_metrics_batch_empty_array_rejected():
    resp = client.post("/metrics/batch", json={"metrics": []})
    assert resp.status_code == 400


def test_post_metrics_batch_missing_metrics_field():
    resp = client.post("/metrics/batch", json={})
    assert resp.status_code == 400


def test_post_metrics_batch_non_object_body():
    resp = client.post("/metrics/batch", json=[1, 2, 3])
    assert resp.status_code == 400


def test_post_metrics_batch_non_object_item():
    resp = client.post("/metrics/batch", json={"metrics": ["not-an-object"]})
    assert resp.status_code == 400
    data = resp.json()
    assert data["rejected_count"] == 1
    assert data["rejected"][0]["index"] == 0


def test_post_metrics_batch_exceeds_max_size():
    metrics = [
        {"service": f"s{i}", "status": "healthy", "response_time_ms": 1.0}
        for i in range(501)
    ]
    resp = client.post("/metrics/batch", json={"metrics": metrics})
    assert resp.status_code == 400
    assert "at most" in resp.json()["detail"]


def test_post_metrics_batch_respects_max_size():
    metrics = [
        {"service": f"s{i}", "status": "healthy", "response_time_ms": 1.0}
        for i in range(500)
    ]
    resp = client.post("/metrics/batch", json={"metrics": metrics})
    assert resp.status_code == 201
    data = resp.json()
    assert data["accepted_count"] == 500


# --- /metrics/overview -------------------------------------------------------

def test_overview_empty_store():
    resp = client.get("/metrics/overview")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_records"] == 0
    assert data["services_count"] == 0
    assert data["overall_uptime_pct"] == 0.0
    assert data["status_counts"] == {
        "healthy": 0,
        "unhealthy": 0,
        "degraded": 0,
        "unknown": 0,
    }
    assert data["earliest_timestamp"] is None
    assert data["latest_timestamp"] is None
    assert data["response_time_ms"]["avg"] == 0.0


def test_overview_aggregates_across_services():
    client.post("/metrics", json={"service": "a", "status": "healthy", "response_time_ms": 10, "timestamp": 100.0})
    client.post("/metrics", json={"service": "a", "status": "unhealthy", "response_time_ms": 30, "timestamp": 200.0})
    client.post("/metrics", json={"service": "b", "status": "healthy", "response_time_ms": 20, "timestamp": 300.0})
    resp = client.get("/metrics/overview")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_records"] == 3
    assert data["services_count"] == 2
    assert data["status_counts"]["healthy"] == 2
    assert data["status_counts"]["unhealthy"] == 1
    # 2 of 3 healthy
    assert data["overall_uptime_pct"] == 66.67
    assert data["earliest_timestamp"] == 100.0
    assert data["latest_timestamp"] == 300.0
    rt = data["response_time_ms"]
    assert rt["min"] == 10.0
    assert rt["max"] == 30.0
    assert rt["avg"] == 20.0


def test_overview_filter_by_service():
    client.post("/metrics", json={"service": "a", "status": "healthy", "response_time_ms": 10})
    client.post("/metrics", json={"service": "b", "status": "unhealthy", "response_time_ms": 50})
    resp = client.get("/metrics/overview?service=a")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_records"] == 1
    assert data["services_count"] == 1
    assert data["overall_uptime_pct"] == 100.0


def test_overview_filter_by_status():
    client.post("/metrics", json={"service": "a", "status": "healthy", "response_time_ms": 10})
    client.post("/metrics", json={"service": "a", "status": "degraded", "response_time_ms": 40})
    resp = client.get("/metrics/overview?status=degraded")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_records"] == 1
    assert data["status_counts"]["degraded"] == 1
    assert data["status_counts"]["healthy"] == 0


def test_overview_filter_by_time_range():
    for ts in (100.0, 200.0, 300.0):
        client.post(
            "/metrics",
            json={"service": "a", "status": "healthy", "response_time_ms": ts / 10, "timestamp": ts},
        )
    resp = client.get("/metrics/overview?since=150&until=250")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_records"] == 1
    assert data["earliest_timestamp"] == 200.0
    assert data["latest_timestamp"] == 200.0


def test_overview_invalid_range():
    resp = client.get("/metrics/overview?since=200&until=100")
    assert resp.status_code == 400


def test_overview_invalid_status():
    resp = client.get("/metrics/overview?status=bogus")
    assert resp.status_code == 422


def test_overview_filter_by_q_partial_match():
    client.post("/metrics", json={"service": "payments-api", "status": "healthy", "response_time_ms": 10})
    client.post("/metrics", json={"service": "payments-worker", "status": "healthy", "response_time_ms": 20})
    client.post("/metrics", json={"service": "orders-api", "status": "unhealthy", "response_time_ms": 30})
    resp = client.get("/metrics/overview?q=payments")
    assert resp.status_code == 200
    data = resp.json()
    # payments-api と payments-worker のみカウント
    assert data["total_records"] == 2
    assert data["services_count"] == 2


def test_overview_q_case_insensitive():
    client.post("/metrics", json={"service": "Payments-API", "status": "healthy", "response_time_ms": 10})
    resp = client.get("/metrics/overview?q=PAYMENTS")
    assert resp.status_code == 200
    assert resp.json()["total_records"] == 1


def test_overview_q_blank_rejected():
    # 既存 /metrics と挙動を揃え、trim 後が空の q は 400 を返す。
    resp = client.get("/metrics/overview?q=%20%20")
    assert resp.status_code == 400
    assert "blank" in resp.json()["detail"]


def test_overview_q_too_long_rejected():
    resp = client.get("/metrics/overview?q=" + "x" * 9999)
    assert resp.status_code == 400


def test_overview_store_unit_percentiles():
    s = MetricsStore()
    for i in range(1, 101):
        s.add(MetricRecord(
            service="svc", status="healthy", response_time_ms=float(i), timestamp=float(i)
        ))
    result = s.overview()
    assert result["total_records"] == 100
    assert result["services_count"] == 1
    assert result["response_time_ms"]["p50"] == 50.5
    assert result["response_time_ms"]["p95"] == 95.05
    assert result["overall_uptime_pct"] == 100.0



def _seed_for_q(services: list[str]) -> None:
    for svc in services:
        client.post(
            "/metrics",
            json={"service": svc, "status": "healthy", "response_time_ms": 10.0},
        )


def test_list_metrics_q_substring_case_insensitive():
    _seed_for_q(["payments-api", "payments-worker", "auth-api", "scheduler"])
    resp = client.get("/metrics?q=PAYMENTS")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 2
    assert {m["service"] for m in data["metrics"]} == {"payments-api", "payments-worker"}


def test_list_metrics_q_no_match():
    _seed_for_q(["auth-api", "scheduler"])
    resp = client.get("/metrics?q=payments")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 0
    assert data["count"] == 0
    assert data["metrics"] == []


def test_list_metrics_q_combined_with_service_filter_is_and():
    _seed_for_q(["payments-api", "payments-worker", "auth-api"])
    # service=payments-api（完全一致）かつ q=worker（部分一致） → 0 件
    resp = client.get("/metrics?service=payments-api&q=worker")
    assert resp.status_code == 200
    assert resp.json()["total"] == 0
    # service=payments-api かつ q=payments → 1 件（payments-api のみ）
    resp = client.get("/metrics?service=payments-api&q=payments")
    assert resp.status_code == 200
    assert resp.json()["total"] == 1


def test_list_metrics_q_blank_returns_400():
    resp = client.get("/metrics?q=%20%20")
    assert resp.status_code == 400
    assert "blank" in resp.json()["detail"]


def test_list_metrics_q_too_long_returns_400():
    long_q = "x" * 101  # MAX_SERVICE_LENGTH = 100
    resp = client.get(f"/metrics?q={long_q}")
    assert resp.status_code == 400
    assert "100" in resp.json()["detail"]


def test_list_services_q_substring_case_insensitive():
    _seed_for_q(["payments-api", "payments-worker", "auth-api", "scheduler"])
    resp = client.get("/metrics/services?q=Payments")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 2
    assert {s["service"] for s in data["services"]} == {"payments-api", "payments-worker"}


def test_list_services_q_combined_with_service_filter():
    _seed_for_q(["payments-api", "payments-worker", "auth-api"])
    resp = client.get("/metrics/services?service=payments-api&q=payments")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert data["services"][0]["service"] == "payments-api"


def test_list_services_q_blank_returns_400():
    resp = client.get("/metrics/services?q=%20%20")
    assert resp.status_code == 400
