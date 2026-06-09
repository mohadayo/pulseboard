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
    # service / before / status のいずれも未指定の場合は 400 で拒否される
    resp = client.delete("/metrics")
    assert resp.status_code == 400
    detail = resp.json()["detail"]
    assert "service" in detail
    assert "before" in detail
    assert "status" in detail


def test_delete_metrics_by_status_only():
    # status のみ指定で unhealthy のレコードだけが削除されること
    client.post("/metrics", json={"service": "web", "status": "healthy", "response_time_ms": 1})
    client.post("/metrics", json={"service": "db", "status": "unhealthy", "response_time_ms": 2})
    client.post("/metrics", json={"service": "cache", "status": "unhealthy", "response_time_ms": 3})

    resp = client.delete("/metrics?status=unhealthy")
    assert resp.status_code == 200
    data = resp.json()
    assert data["deleted_count"] == 2
    assert data["status"] == "unhealthy"
    assert data["service"] is None
    assert data["before"] is None

    remaining = client.get("/metrics").json()
    assert remaining["total"] == 1
    assert remaining["metrics"][0]["service"] == "web"


def test_delete_metrics_by_status_and_before_combined():
    # status と before の AND で「古い unhealthy」だけ削除されること
    base = time.time()
    client.post("/metrics", json={
        "service": "a", "status": "unhealthy",
        "response_time_ms": 1, "timestamp": base - 1000,
    })
    client.post("/metrics", json={
        "service": "b", "status": "unhealthy",
        "response_time_ms": 2, "timestamp": base,
    })
    client.post("/metrics", json={
        "service": "c", "status": "healthy",
        "response_time_ms": 3, "timestamp": base - 1000,
    })

    cutoff = base - 500
    resp = client.delete(f"/metrics?status=unhealthy&before={cutoff}")
    assert resp.status_code == 200
    data = resp.json()
    # a だけ（古い かつ unhealthy）が削除される
    assert data["deleted_count"] == 1
    assert data["status"] == "unhealthy"
    assert data["before"] == cutoff

    remaining = client.get("/metrics").json()
    remaining_services = sorted(m["service"] for m in remaining["metrics"])
    assert remaining_services == ["b", "c"]


def test_delete_metrics_rejects_invalid_status():
    # status は Literal で 422
    resp = client.delete("/metrics?status=broken")
    assert resp.status_code == 422


def test_delete_metrics_by_status_no_match_returns_zero():
    client.post("/metrics", json={"service": "a", "status": "healthy", "response_time_ms": 1})
    resp = client.delete("/metrics?status=degraded")
    assert resp.status_code == 200
    data = resp.json()
    assert data["deleted_count"] == 0
    assert "error" in data
    assert data["status"] == "degraded"


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

    # status のみ
    s = MetricsStore()
    s.add(MetricRecord(service="a", status="healthy", response_time_ms=10, timestamp=100))
    s.add(MetricRecord(service="b", status="unhealthy", response_time_ms=20, timestamp=100))
    s.add(MetricRecord(service="c", status="unhealthy", response_time_ms=30, timestamp=200))
    assert s.delete(status="unhealthy") == 2
    assert [r.service for r in s.get_all()] == ["a"]

    # service + before + status の三段 AND
    s = MetricsStore()
    s.add(MetricRecord(service="a", status="unhealthy", response_time_ms=10, timestamp=100))
    s.add(MetricRecord(service="a", status="healthy", response_time_ms=20, timestamp=100))
    s.add(MetricRecord(service="a", status="unhealthy", response_time_ms=30, timestamp=300))
    s.add(MetricRecord(service="b", status="unhealthy", response_time_ms=40, timestamp=100))
    # 「a の 古い (before=200) unhealthy」のみ 1 件削除
    assert s.delete(service="a", before=200, status="unhealthy") == 1
    remaining = sorted((r.service, r.status, r.timestamp) for r in s.get_all())
    assert remaining == [
        ("a", "healthy", 100),
        ("a", "unhealthy", 300),
        ("b", "unhealthy", 100),
    ]


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
    # latest_response_ms は last_seen 時点（timestamp=200.0）の観測値
    assert by_name["web"]["latest_response_ms"] == 2.0
    assert by_name["db"]["total_checks"] == 1
    assert by_name["db"]["latest_status"] == "healthy"
    assert by_name["db"]["latest_response_ms"] == 3.0


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


# ---------------------------------------------------------------------------
# GET /metrics/count
# ---------------------------------------------------------------------------


def _seed_for_count():
    """各ステータスを混ぜたサンプルレコードを投入する。"""
    samples = [
        ("api", "healthy", 10.0, 100.0),
        ("api", "healthy", 12.0, 110.0),
        ("api", "unhealthy", 200.0, 120.0),
        ("db", "healthy", 5.0, 130.0),
        ("db", "degraded", 80.0, 140.0),
        ("worker", "unknown", 0.0, 150.0),
    ]
    for service, status, rt, ts in samples:
        client.post(
            "/metrics",
            json={
                "service": service, "status": status,
                "response_time_ms": rt, "timestamp": ts,
            },
        )


def test_count_empty_store():
    resp = client.get("/metrics/count")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 0
    # 全ステータスのキーが 0 で初期化されていること（クライアントの存在チェック不要）
    assert data["by_status"] == {
        "healthy": 0, "unhealthy": 0, "degraded": 0, "unknown": 0,
    }


def test_count_all_records():
    _seed_for_count()
    resp = client.get("/metrics/count")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 6
    assert data["by_status"] == {
        "healthy": 3, "unhealthy": 1, "degraded": 1, "unknown": 1,
    }


def test_count_filtered_by_service():
    _seed_for_count()
    resp = client.get("/metrics/count?service=api")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 3
    assert data["by_status"]["healthy"] == 2
    assert data["by_status"]["unhealthy"] == 1
    assert data["by_status"]["degraded"] == 0
    assert data["by_status"]["unknown"] == 0


def test_count_filtered_by_status():
    _seed_for_count()
    resp = client.get("/metrics/count?status=healthy")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 3
    # status 絞り込み後でも by_status はキーを全て含む
    assert data["by_status"]["healthy"] == 3
    assert data["by_status"]["unhealthy"] == 0


def test_count_filtered_by_time_range():
    _seed_for_count()
    resp = client.get("/metrics/count?since=110&until=140")
    assert resp.status_code == 200
    data = resp.json()
    # ts=110,120,130,140 → 4 件
    assert data["total"] == 4


def test_count_q_substring_case_insensitive():
    _seed_for_count()
    resp = client.get("/metrics/count?q=API")
    assert resp.status_code == 200
    data = resp.json()
    # service=api のレコード 3 件のみ
    assert data["total"] == 3
    assert data["by_status"]["healthy"] == 2
    assert data["by_status"]["unhealthy"] == 1


def test_count_invalid_time_range_returns_400():
    resp = client.get("/metrics/count?since=200&until=100")
    assert resp.status_code == 400
    assert "since" in resp.json()["detail"]


def test_count_blank_q_returns_400():
    resp = client.get("/metrics/count?q=%20%20")
    assert resp.status_code == 400


def test_count_invalid_status_returns_422():
    # FastAPI の Query Literal 検査により未知 status は 422 になる
    resp = client.get("/metrics/count?status=bogus")
    assert resp.status_code == 422


def test_list_services_latest_response_ms_single_observation():
    # 単一観測なら、その観測の response_time_ms が latest_response_ms になる
    client.post("/metrics", json={
        "service": "single", "status": "healthy", "response_time_ms": 12.5, "timestamp": 100.0,
    })
    resp = client.get("/metrics/services")
    services = resp.json()["services"]
    assert services[0]["service"] == "single"
    assert services[0]["latest_response_ms"] == 12.5


def test_list_services_latest_response_ms_older_observation_does_not_overwrite():
    # 後から古いタイムスタンプの観測を追加しても、latest_response_ms は更新されない
    client.post("/metrics", json={
        "service": "web", "status": "healthy", "response_time_ms": 50.0, "timestamp": 200.0,
    })
    client.post("/metrics", json={
        "service": "web", "status": "unhealthy", "response_time_ms": 999.0, "timestamp": 100.0,
    })
    resp = client.get("/metrics/services")
    by_name = {s["service"]: s for s in resp.json()["services"]}
    assert by_name["web"]["latest_response_ms"] == 50.0
    assert by_name["web"]["latest_status"] == "healthy"


def test_list_services_latest_response_ms_is_rounded():
    # 内部値は小数点 2 桁に丸めて返す（avg_response_ms 等と同じ流儀）
    client.post("/metrics", json={
        "service": "round", "status": "healthy",
        "response_time_ms": 12.345678, "timestamp": 100.0,
    })
    resp = client.get("/metrics/services")
    services = resp.json()["services"]
    assert services[0]["latest_response_ms"] == 12.35


def test_list_services_sort_by_latest_response_ms_asc():
    # latest_response_ms を昇順にソートできること（最も速いサービスが先頭）。
    client.post("/metrics", json={
        "service": "slow", "status": "healthy",
        "response_time_ms": 500.0, "timestamp": 100.0,
    })
    client.post("/metrics", json={
        "service": "fast", "status": "healthy",
        "response_time_ms": 10.0, "timestamp": 100.0,
    })
    client.post("/metrics", json={
        "service": "mid", "status": "healthy",
        "response_time_ms": 100.0, "timestamp": 100.0,
    })
    resp = client.get("/metrics/services?sort=latest_response_ms&order=asc")
    assert resp.status_code == 200
    services = resp.json()["services"]
    assert [s["service"] for s in services] == ["fast", "mid", "slow"]


def test_list_services_sort_by_latest_response_ms_desc():
    # latest_response_ms を降順にソートできること（最も遅いサービスが先頭）。
    client.post("/metrics", json={
        "service": "slow", "status": "healthy",
        "response_time_ms": 500.0, "timestamp": 100.0,
    })
    client.post("/metrics", json={
        "service": "fast", "status": "healthy",
        "response_time_ms": 10.0, "timestamp": 100.0,
    })
    client.post("/metrics", json={
        "service": "mid", "status": "healthy",
        "response_time_ms": 100.0, "timestamp": 100.0,
    })
    resp = client.get("/metrics/services?sort=latest_response_ms&order=desc")
    assert resp.status_code == 200
    services = resp.json()["services"]
    assert [s["service"] for s in services] == ["slow", "mid", "fast"]


def test_list_services_sort_by_latest_response_ms_uses_latest_observation():
    # 同一サービスに複数観測がある場合、最新タイムスタンプの observation の
    # response_time_ms が並び替えに使われること（latest_response_ms と整合）。
    # web: 最終観測 = 200ms / db: 最終観測 = 50ms
    client.post("/metrics", json={
        "service": "web", "status": "healthy",
        "response_time_ms": 5.0, "timestamp": 100.0,
    })
    client.post("/metrics", json={
        "service": "web", "status": "healthy",
        "response_time_ms": 200.0, "timestamp": 200.0,
    })
    client.post("/metrics", json={
        "service": "db", "status": "healthy",
        "response_time_ms": 999.0, "timestamp": 100.0,
    })
    client.post("/metrics", json={
        "service": "db", "status": "healthy",
        "response_time_ms": 50.0, "timestamp": 300.0,
    })
    resp = client.get("/metrics/services?sort=latest_response_ms&order=asc")
    assert resp.status_code == 200
    services = resp.json()["services"]
    assert [s["service"] for s in services] == ["db", "web"]


def test_get_service_detail_404_when_no_data():
    resp = client.get("/metrics/services/unknown")
    assert resp.status_code == 404
    assert "unknown" in resp.json()["detail"]


def test_get_service_detail_returns_aggregate():
    for i, rt in enumerate([10.0, 30.0, 20.0, 40.0]):
        client.post("/metrics", json={
            "service": "api", "status": "healthy" if i % 2 == 0 else "unhealthy",
            "response_time_ms": rt, "timestamp": 100.0 + i,
        })
    resp = client.get("/metrics/services/api")
    assert resp.status_code == 200
    data = resp.json()
    assert data["service"] == "api"
    assert data["total_checks"] == 4
    assert data["healthy_checks"] == 2
    assert data["uptime_pct"] == 50.0
    assert data["min_response_ms"] == 10.0
    assert data["max_response_ms"] == 40.0
    assert data["avg_response_ms"] == 25.0
    # 最新観測は timestamp=103 のもの: rt=40.0, status=unhealthy
    assert data["latest_status"] == "unhealthy"
    assert data["latest_response_ms"] == 40.0
    assert data["first_seen"] == 100.0
    assert data["last_seen"] == 103.0
    # percentile keys must exist
    assert "p50_response_ms" in data
    assert "p95_response_ms" in data
    assert "p99_response_ms" in data


def test_get_service_detail_filters_other_services():
    client.post("/metrics", json={
        "service": "api", "status": "healthy", "response_time_ms": 10.0,
    })
    client.post("/metrics", json={
        "service": "db", "status": "unhealthy", "response_time_ms": 500.0,
    })
    resp = client.get("/metrics/services/api")
    assert resp.status_code == 200
    data = resp.json()
    assert data["service"] == "api"
    assert data["total_checks"] == 1
    assert data["healthy_checks"] == 1


def test_get_service_detail_strips_whitespace():
    client.post("/metrics", json={
        "service": "web", "status": "healthy", "response_time_ms": 10.0,
    })
    resp = client.get("/metrics/services/%20web%20")
    assert resp.status_code == 200
    assert resp.json()["service"] == "web"


def test_get_service_detail_rejects_blank_name():
    resp = client.get("/metrics/services/%20%20")
    assert resp.status_code == 400


def test_get_service_detail_rejects_overlong_name():
    long_name = "x" * 200
    resp = client.get(f"/metrics/services/{long_name}")
    assert resp.status_code == 400


def test_get_service_detail_time_range_filter():
    for ts, rt in [(100.0, 10.0), (200.0, 20.0), (300.0, 30.0)]:
        client.post("/metrics", json={
            "service": "web", "status": "healthy", "response_time_ms": rt, "timestamp": ts,
        })
    resp = client.get("/metrics/services/web?since=150&until=250")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_checks"] == 1
    assert data["avg_response_ms"] == 20.0


def test_get_service_detail_since_greater_than_until_rejected():
    resp = client.get("/metrics/services/web?since=300&until=100")
    assert resp.status_code == 400


def test_get_service_detail_404_when_filter_excludes_all():
    client.post("/metrics", json={
        "service": "web", "status": "healthy", "response_time_ms": 10.0, "timestamp": 50.0,
    })
    resp = client.get("/metrics/services/web?since=100")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# /metrics/timeseries — 時系列バケット集計
# ---------------------------------------------------------------------------


def test_timeseries_empty_returns_no_buckets():
    resp = client.get("/metrics/timeseries")
    assert resp.status_code == 200
    data = resp.json()
    assert data["bucket_seconds"] == 60
    assert data["count"] == 0
    assert data["buckets"] == []


def test_timeseries_default_bucket_size_groups_within_minute():
    # 60 秒幅（default）。バケット [1020, 1080) に 3 件、[1080, 1140) に 1 件。
    client.post("/metrics", json={
        "service": "web", "status": "healthy", "response_time_ms": 10.0, "timestamp": 1020.0,
    })
    client.post("/metrics", json={
        "service": "web", "status": "unhealthy", "response_time_ms": 30.0, "timestamp": 1050.0,
    })
    client.post("/metrics", json={
        "service": "web", "status": "healthy", "response_time_ms": 50.0, "timestamp": 1079.0,
    })
    client.post("/metrics", json={
        "service": "web", "status": "healthy", "response_time_ms": 70.0, "timestamp": 1080.0,
    })
    resp = client.get("/metrics/timeseries")
    assert resp.status_code == 200
    data = resp.json()
    assert data["bucket_seconds"] == 60
    assert data["count"] == 2
    buckets = data["buckets"]
    assert buckets[0]["bucket_start"] == 1020.0
    assert buckets[0]["total"] == 3
    assert buckets[0]["by_status"]["healthy"] == 2
    assert buckets[0]["by_status"]["unhealthy"] == 1
    assert buckets[0]["avg_response_ms"] == 30.0  # (10+30+50)/3
    assert buckets[1]["bucket_start"] == 1080.0
    assert buckets[1]["total"] == 1
    assert buckets[1]["avg_response_ms"] == 70.0


def test_timeseries_custom_bucket_size():
    client.post("/metrics", json={
        "service": "a", "status": "healthy", "response_time_ms": 1.0, "timestamp": 100.0,
    })
    client.post("/metrics", json={
        "service": "a", "status": "healthy", "response_time_ms": 1.0, "timestamp": 109.0,
    })
    client.post("/metrics", json={
        "service": "a", "status": "healthy", "response_time_ms": 1.0, "timestamp": 110.0,
    })
    resp = client.get("/metrics/timeseries?bucket_seconds=10")
    assert resp.status_code == 200
    data = resp.json()
    assert data["bucket_seconds"] == 10
    assert data["count"] == 2
    assert data["buckets"][0]["bucket_start"] == 100.0
    assert data["buckets"][0]["total"] == 2
    assert data["buckets"][1]["bucket_start"] == 110.0
    assert data["buckets"][1]["total"] == 1


def test_timeseries_by_status_includes_all_keys():
    client.post("/metrics", json={
        "service": "a", "status": "healthy", "response_time_ms": 1.0, "timestamp": 1.0,
    })
    resp = client.get("/metrics/timeseries?bucket_seconds=60")
    data = resp.json()
    by_status = data["buckets"][0]["by_status"]
    assert set(by_status.keys()) == {"healthy", "unhealthy", "degraded", "unknown"}
    assert by_status["healthy"] == 1
    assert by_status["unhealthy"] == 0
    assert by_status["degraded"] == 0
    assert by_status["unknown"] == 0


def test_timeseries_sparse_skips_empty_buckets():
    # 60 秒バケットで時刻 60 と時刻 600 にレコード → 中間バケットは含まれない
    client.post("/metrics", json={
        "service": "a", "status": "healthy", "response_time_ms": 1.0, "timestamp": 60.0,
    })
    client.post("/metrics", json={
        "service": "a", "status": "healthy", "response_time_ms": 1.0, "timestamp": 600.0,
    })
    resp = client.get("/metrics/timeseries?bucket_seconds=60")
    data = resp.json()
    assert data["count"] == 2
    starts = [b["bucket_start"] for b in data["buckets"]]
    assert starts == [60.0, 600.0]


def test_timeseries_filter_by_service():
    client.post("/metrics", json={
        "service": "a", "status": "healthy", "response_time_ms": 10.0, "timestamp": 100.0,
    })
    client.post("/metrics", json={
        "service": "b", "status": "healthy", "response_time_ms": 20.0, "timestamp": 100.0,
    })
    resp = client.get("/metrics/timeseries?service=a&bucket_seconds=60")
    data = resp.json()
    assert data["count"] == 1
    assert data["buckets"][0]["total"] == 1
    assert data["buckets"][0]["avg_response_ms"] == 10.0


def test_timeseries_filter_by_status():
    client.post("/metrics", json={
        "service": "a", "status": "healthy", "response_time_ms": 10.0, "timestamp": 100.0,
    })
    client.post("/metrics", json={
        "service": "a", "status": "unhealthy", "response_time_ms": 20.0, "timestamp": 100.0,
    })
    resp = client.get("/metrics/timeseries?status=healthy&bucket_seconds=60")
    data = resp.json()
    assert data["buckets"][0]["total"] == 1
    assert data["buckets"][0]["by_status"]["healthy"] == 1
    assert data["buckets"][0]["by_status"]["unhealthy"] == 0


def test_timeseries_filter_by_since_until():
    for ts in (50.0, 100.0, 150.0, 200.0):
        client.post("/metrics", json={
            "service": "a", "status": "healthy", "response_time_ms": 1.0, "timestamp": ts,
        })
    resp = client.get("/metrics/timeseries?since=100&until=150&bucket_seconds=60")
    data = resp.json()
    total = sum(b["total"] for b in data["buckets"])
    assert total == 2


def test_timeseries_filter_by_q_partial_match():
    client.post("/metrics", json={
        "service": "frontend-web", "status": "healthy", "response_time_ms": 1.0, "timestamp": 1.0,
    })
    client.post("/metrics", json={
        "service": "backend-api", "status": "healthy", "response_time_ms": 1.0, "timestamp": 1.0,
    })
    resp = client.get("/metrics/timeseries?q=front&bucket_seconds=60")
    data = resp.json()
    assert data["buckets"][0]["total"] == 1


def test_timeseries_rejects_bucket_too_small():
    resp = client.get("/metrics/timeseries?bucket_seconds=0")
    assert resp.status_code == 422


def test_timeseries_rejects_negative_bucket():
    resp = client.get("/metrics/timeseries?bucket_seconds=-1")
    assert resp.status_code == 422


def test_timeseries_rejects_bucket_too_large():
    resp = client.get("/metrics/timeseries?bucket_seconds=86401")
    assert resp.status_code == 422


def test_timeseries_rejects_since_greater_than_until():
    resp = client.get("/metrics/timeseries?since=200&until=100")
    assert resp.status_code == 400


def test_timeseries_rejects_blank_q():
    resp = client.get("/metrics/timeseries?q=%20%20%20")
    assert resp.status_code == 400


def test_timeseries_avg_response_ms_rounded_to_two_decimals():
    client.post("/metrics", json={
        "service": "a", "status": "healthy", "response_time_ms": 1.0, "timestamp": 1.0,
    })
    client.post("/metrics", json={
        "service": "a", "status": "healthy", "response_time_ms": 2.0, "timestamp": 2.0,
    })
    client.post("/metrics", json={
        "service": "a", "status": "healthy", "response_time_ms": 3.0, "timestamp": 3.0,
    })
    # (1+2+3)/3 = 2.0
    resp = client.get("/metrics/timeseries?bucket_seconds=60")
    data = resp.json()
    assert data["buckets"][0]["avg_response_ms"] == 2.0


def test_timeseries_buckets_sorted_ascending():
    # ランダム順で投入してもバケットは昇順で返るべき
    for ts in (300.0, 100.0, 500.0, 200.0):
        client.post("/metrics", json={
            "service": "a", "status": "healthy", "response_time_ms": 1.0, "timestamp": ts,
        })
    resp = client.get("/metrics/timeseries?bucket_seconds=60")
    starts = [b["bucket_start"] for b in resp.json()["buckets"]]
    assert starts == sorted(starts)


def test_timeseries_bucket_includes_min_max_and_percentiles():
    # 1 バケット内の応答時間 [10, 20, 30, 40, 50] に対する min/max/percentile
    # を SLA ダッシュボード用に返すこと。値は service_detail と同じ
    # `_percentile` 実装（線形補間）に揃える。
    for rt in (10.0, 20.0, 30.0, 40.0, 50.0):
        client.post("/metrics", json={
            "service": "web", "status": "healthy", "response_time_ms": rt, "timestamp": 1000.0,
        })
    resp = client.get("/metrics/timeseries?bucket_seconds=60")
    assert resp.status_code == 200
    bucket = resp.json()["buckets"][0]
    assert bucket["total"] == 5
    assert bucket["min_response_ms"] == 10.0
    assert bucket["max_response_ms"] == 50.0
    # 5 要素の p50 は中央値 30.0
    assert bucket["p50_response_ms"] == 30.0
    # 95th 線形補間: rank = 0.95*(5-1) = 3.8 → values[3]*0.2 + values[4]*0.8 = 40*0.2 + 50*0.8 = 48.0
    assert bucket["p95_response_ms"] == 48.0
    # 99th: rank = 0.99*4 = 3.96 → 40*0.04 + 50*0.96 = 49.6
    assert bucket["p99_response_ms"] == 49.6


def test_timeseries_single_record_bucket_min_max_percentiles_collapse():
    # 1 件のみのバケットは min/max/percentile すべて同じ値になること。
    client.post("/metrics", json={
        "service": "a", "status": "healthy", "response_time_ms": 42.5, "timestamp": 1.0,
    })
    resp = client.get("/metrics/timeseries?bucket_seconds=60")
    bucket = resp.json()["buckets"][0]
    assert bucket["min_response_ms"] == 42.5
    assert bucket["max_response_ms"] == 42.5
    assert bucket["p50_response_ms"] == 42.5
    assert bucket["p95_response_ms"] == 42.5
    assert bucket["p99_response_ms"] == 42.5


def test_timeseries_per_bucket_percentiles_are_independent():
    # 各バケットで集計が独立していること（前バケットの値が引き継がれない）。
    # バケット A (bucket_start=60): [1, 9] → min=1, max=9, p50=5.0
    # バケット B (bucket_start=120): [100, 900] → min=100, max=900, p50=500.0
    client.post("/metrics", json={
        "service": "a", "status": "healthy", "response_time_ms": 1.0, "timestamp": 60.0,
    })
    client.post("/metrics", json={
        "service": "a", "status": "healthy", "response_time_ms": 9.0, "timestamp": 90.0,
    })
    client.post("/metrics", json={
        "service": "a", "status": "healthy", "response_time_ms": 100.0, "timestamp": 120.0,
    })
    client.post("/metrics", json={
        "service": "a", "status": "healthy", "response_time_ms": 900.0, "timestamp": 150.0,
    })
    resp = client.get("/metrics/timeseries?bucket_seconds=60")
    buckets = resp.json()["buckets"]
    assert buckets[0]["min_response_ms"] == 1.0
    assert buckets[0]["max_response_ms"] == 9.0
    assert buckets[0]["p50_response_ms"] == 5.0
    assert buckets[1]["min_response_ms"] == 100.0
    assert buckets[1]["max_response_ms"] == 900.0
    assert buckets[1]["p50_response_ms"] == 500.0


def test_timeseries_existing_fields_unchanged():
    # 既存フィールド (bucket_start / total / by_status / avg_response_ms) の
    # 形と値が新フィールド追加で壊れていないこと（後方互換の回帰）。
    client.post("/metrics", json={
        "service": "a", "status": "healthy", "response_time_ms": 10.0, "timestamp": 60.0,
    })
    client.post("/metrics", json={
        "service": "a", "status": "unhealthy", "response_time_ms": 30.0, "timestamp": 90.0,
    })
    resp = client.get("/metrics/timeseries?bucket_seconds=60")
    bucket = resp.json()["buckets"][0]
    assert bucket["bucket_start"] == 60.0
    assert bucket["total"] == 2
    assert bucket["by_status"]["healthy"] == 1
    assert bucket["by_status"]["unhealthy"] == 1
    assert bucket["avg_response_ms"] == 20.0


# ---------------------------------------------------------------------------
# GET /metrics/services/names — distinct service 名のみを返す軽量エンドポイント
# ---------------------------------------------------------------------------


def test_service_names_empty_store():
    resp = client.get("/metrics/services/names")
    assert resp.status_code == 200
    data = resp.json()
    assert data == {
        "count": 0,
        "total": 0,
        "limit": data["limit"],
        "offset": 0,
        "order": "asc",
        "names": [],
    }


def test_service_names_distinct_and_sorted_asc():
    # 同じ service を複数回投入しても 1 件にまとめられる、かつ昇順で返る
    for svc in ("zeta", "alpha", "beta", "alpha", "beta"):
        client.post("/metrics", json={
            "service": svc, "status": "healthy", "response_time_ms": 1.0, "timestamp": 1.0,
        })
    resp = client.get("/metrics/services/names")
    assert resp.status_code == 200
    data = resp.json()
    assert data["names"] == ["alpha", "beta", "zeta"]
    assert data["total"] == 3
    assert data["count"] == 3


def test_service_names_order_desc():
    for svc in ("alpha", "beta", "zeta"):
        client.post("/metrics", json={
            "service": svc, "status": "healthy", "response_time_ms": 1.0, "timestamp": 1.0,
        })
    resp = client.get("/metrics/services/names?order=desc")
    assert resp.status_code == 200
    assert resp.json()["names"] == ["zeta", "beta", "alpha"]


def test_service_names_pagination():
    for svc in ("a", "b", "c", "d", "e"):
        client.post("/metrics", json={
            "service": svc, "status": "healthy", "response_time_ms": 1.0, "timestamp": 1.0,
        })
    resp = client.get("/metrics/services/names?limit=2&offset=1")
    assert resp.status_code == 200
    data = resp.json()
    assert data["names"] == ["b", "c"]
    assert data["count"] == 2
    assert data["total"] == 5
    assert data["limit"] == 2
    assert data["offset"] == 1


def test_service_names_q_filter_case_insensitive():
    for svc in ("api-gateway", "API-Worker", "user-service", "BillingAPI"):
        client.post("/metrics", json={
            "service": svc, "status": "healthy", "response_time_ms": 1.0, "timestamp": 1.0,
        })
    resp = client.get("/metrics/services/names?q=api")
    assert resp.status_code == 200
    data = resp.json()
    # "api" を含む 3 件のみ（大文字小文字無視）。元の表記は保たれる。
    assert set(data["names"]) == {"api-gateway", "API-Worker", "BillingAPI"}
    assert data["total"] == 3


def test_service_names_q_blank_rejected():
    resp = client.get("/metrics/services/names?q=%20%20")
    assert resp.status_code == 400
    assert "must not be blank" in resp.json()["detail"]


def test_service_names_since_until_filter():
    client.post("/metrics", json={
        "service": "old", "status": "healthy", "response_time_ms": 1.0, "timestamp": 100.0,
    })
    client.post("/metrics", json={
        "service": "new", "status": "healthy", "response_time_ms": 1.0, "timestamp": 200.0,
    })
    resp = client.get("/metrics/services/names?since=150")
    assert resp.status_code == 200
    assert resp.json()["names"] == ["new"]


def test_service_names_since_greater_than_until_rejected():
    resp = client.get("/metrics/services/names?since=200&until=100")
    assert resp.status_code == 400
    assert "since must be less than or equal to until" in resp.json()["detail"]


def test_service_names_does_not_collide_with_detail_route():
    # ルート定義順により、`/metrics/services/names` が
    # `/metrics/services/{service_name}` より優先されて固定 path として
    # 解釈されること。`names` という名前のサービスを投入した状態で確認。
    client.post("/metrics", json={
        "service": "names", "status": "healthy", "response_time_ms": 1.0, "timestamp": 1.0,
    })
    resp = client.get("/metrics/services/names")
    assert resp.status_code == 200
    data = resp.json()
    # 集約 detail オブジェクト（service / total_checks 等）ではなく軽量レスポンスが返ること。
    assert "names" in data and isinstance(data["names"], list)
    assert "uptime_pct" not in data
    assert data["names"] == ["names"]


def test_service_names_limit_capped():
    resp = client.get("/metrics/services/names?limit=99999")
    # METRICS_MAX_LIMIT (既定 1000) を超える指定は 422 で拒否される
    assert resp.status_code == 422
