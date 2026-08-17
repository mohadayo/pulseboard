"""`/metrics/count` エンドポイントの `q` クエリパラメータのバリデーション回帰テスト。

`_normalize_q_param()` は `q` が `MAX_SERVICE_LENGTH = 100` 文字を超えると 400 を
返す共通バリデータだが、`test_main.py` では `/metrics` / `/metrics/summary` /
`/metrics/overview` の 3 エンドポイントでのみ「q が長すぎる」ケースを回帰していた。

`/metrics/count` は `_normalize_q_param()` を経由しているものの、長さ超過時の
レスポンス契約を回帰していなかったため、将来リファクタで誤って共通バリデータの
呼び出しが外された場合に検知できない。本ファイルはその 1 ケースの回帰専用。

`test_main.py` に直接追記しなかったのは、既存ファイルが大きく、単一エンドポイントの
追加テストを分離しておいたほうが差分レビューが読みやすくなるため。既存の
`test_count_blank_q_returns_400`（`test_main.py`）と対になる位置づけ。
"""

from fastapi.testclient import TestClient

from main import app

client = TestClient(app)


def test_count_q_too_long_rejected():
    """`q` が MAX_SERVICE_LENGTH (100) を超えると 400 と `detail` に "100" を含む。

    既存 `test_list_metrics_q_too_long_returns_400`（`/metrics` に対する同種の
    検査）と同じ入力・同じアサーションで、`/metrics/count` に対して契約を固定する。
    """
    long_q = "x" * 101  # MAX_SERVICE_LENGTH = 100
    resp = client.get(f"/metrics/count?q={long_q}")
    assert resp.status_code == 400
    assert "100" in resp.json()["detail"]
