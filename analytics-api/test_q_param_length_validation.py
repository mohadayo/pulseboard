"""`q` パラメータの文字数上限（`MAX_SERVICE_LENGTH`）超過時の 400 応答を検証する。

`_normalize_q_param()` は `q` を受け付ける以下 9 エンドポイント全てで共通の
バリデーションロジック（trim 後が空 → 400、`MAX_SERVICE_LENGTH` 超過 → 400）
として使われている:
  GET /metrics, /metrics/summary, /metrics/overview, /metrics/count,
  /metrics/timeseries, /metrics/services, /metrics/services/names,
  /metrics/incidents, /metrics/uptime

`test_main.py` には「文字数超過 → 400」の経路テストが `/metrics` /
`/metrics/summary` / `/metrics/overview` の 3 エンドポイント分しか無く、
残り 6 エンドポイントではこの経路（`Query` バリデーション経由で共通ヘルパーの
エラーが正しく 400 として伝播するか）が未検証だった。共通ヘルパー自体は
テスト済みでも、各エンドポイントの呼び出し経路は個別に確認しないと、将来の
リファクタでこの検証だけ外れるリグレッションを検知できない。本ファイルは
その欠けていた 6 エンドポイント分を、既存の `test_list_metrics_q_too_long_returns_400`
と同じスタイルで補う。
"""

from fastapi.testclient import TestClient

from main import app, store

client = TestClient(app)


def setup_function():
    store.records.clear()


def test_count_q_too_long_returns_400():
    long_q = "x" * 101  # MAX_SERVICE_LENGTH = 100
    resp = client.get(f"/metrics/count?q={long_q}")
    assert resp.status_code == 400
    assert "100" in resp.json()["detail"]


def test_timeseries_rejects_too_long_q():
    long_q = "x" * 101  # MAX_SERVICE_LENGTH = 100
    resp = client.get(f"/metrics/timeseries?q={long_q}")
    assert resp.status_code == 400
    assert "100" in resp.json()["detail"]


def test_list_services_q_too_long_returns_400():
    long_q = "x" * 101  # MAX_SERVICE_LENGTH = 100
    resp = client.get(f"/metrics/services?q={long_q}")
    assert resp.status_code == 400
    assert "100" in resp.json()["detail"]


def test_service_names_q_too_long_rejected():
    long_q = "x" * 101  # MAX_SERVICE_LENGTH = 100
    resp = client.get(f"/metrics/services/names?q={long_q}")
    assert resp.status_code == 400
    assert "100" in resp.json()["detail"]


def test_all_incidents_rejects_too_long_q():
    long_q = "x" * 101  # MAX_SERVICE_LENGTH = 100
    resp = client.get(f"/metrics/incidents?q={long_q}")
    assert resp.status_code == 400
    assert "100" in resp.json()["detail"]


def test_all_uptime_rejects_too_long_q():
    long_q = "x" * 101  # MAX_SERVICE_LENGTH = 100
    resp = client.get(f"/metrics/uptime?q={long_q}")
    assert resp.status_code == 400
    assert "100" in resp.json()["detail"]
