# PulseBoard アーキテクチャ概要

このドキュメントは、PulseBoard を構成する各サービスの役割・依存関係・主要な設定項目を
一枚で俯瞰するためのリファレンスです。詳細な API 仕様やコントリビュートの手順は
[`../README.md`](../README.md) および [`../CONTRIBUTING.md`](../CONTRIBUTING.md) を
参照してください。

## 1. 概要

PulseBoard はマイクロサービス構成のリアルタイム・ヘルスモニタリング基盤で、
以下の 3 つの独立したサービスからなります。すべての通信は HTTP/JSON で行われ、
`docker compose` によりローカル環境で一括起動できます。

## 2. サービス構成

| サービス          | 言語 / フレームワーク       | 既定ポート | 役割                                                        |
| ----------------- | --------------------------- | ---------- | ----------------------------------------------------------- |
| `analytics-api`   | Python / FastAPI            | `8001`     | メトリクスの受け付け・集計と分析結果の提供                  |
| `health-checker`  | Go (net/http)               | `8002`     | 対象サービスへの定期・オンデマンドヘルスチェックの実行      |
| `api-gateway`     | TypeScript / Node.js (Express) | `8000`  | 外部クライアント向けの単一入口、上流サービスへのプロキシ    |

すべてのサービスは `/health` エンドポイントを公開しており、コンテナのヘルスチェックと
`health-checker` からの監視対象となっています。

## 3. コンポーネント図

```
                                +---------------------+
                Client / UI --> |     api-gateway     |  :8000
                                |  (TypeScript/Express)|
                                +----------+----------+
                                           |
                          +----------------+----------------+
                          |                                 |
                          v                                 v
                +-----------------+              +----------------------+
                |  analytics-api  |  :8001       |    health-checker    |  :8002
                |  (Python/FastAPI)|             |         (Go)         |
                +--------+--------+              +----------+-----------+
                         ^                                  |
                         |          監視 / メトリクス送信    |
                         +----------------------------------+
```

- `api-gateway` は外部からの唯一の入口として動作し、`/api/*` 配下のリクエストを
  上流の `analytics-api` / `health-checker` へプロキシします。
- `health-checker` は `analytics-api` および `api-gateway` を対象に `/health` の
  ポーリングを行い、必要に応じて `analytics-api` に結果メトリクスを送信します。
- 追加の監視対象は `EXTRA_TARGETS` 環境変数 (JSON 配列) で拡張できます。

## 4. 起動順序とヘルスチェック

`docker-compose.yml` では `depends_on: { condition: service_healthy }` により、
上流サービスがヘルシーになるまで下流サービスの起動を待機します。実効的な起動順序は
以下の通りです。

1. **`analytics-api`** — 単体で起動。`/health` が 200 を返した時点でヘルシー。
2. **`health-checker`** — `analytics-api` がヘルシーになってから起動。
3. **`api-gateway`** — `analytics-api` と `health-checker` の両方がヘルシーになってから起動。

各サービスの healthcheck 設定 (共通):

| 項目              | 値       |
| ----------------- | -------- |
| `interval`        | `10s`    |
| `timeout`         | `5s`     |
| `retries`         | `3`      |
| `start_period`    | `5s`     |

## 5. 主要な環境変数

すべての既定値は `.env.example` に定義されています。ここではドメイン別に整理して
掲載します。詳細なコメントは [`../.env.example`](../.env.example) を参照してください。

### 5.1 ポート

| 変数              | 既定値 | 説明                          |
| ----------------- | ------ | ----------------------------- |
| `ANALYTICS_PORT`  | `8001` | `analytics-api` の待ち受け    |
| `CHECKER_PORT`    | `8002` | `health-checker` の待ち受け   |
| `GATEWAY_PORT`    | `8000` | `api-gateway` の待ち受け      |

### 5.2 サービス間 URL (Docker を使わないローカル開発向け)

| 変数             | 既定値                    |
| ---------------- | ------------------------- |
| `ANALYTICS_URL`  | `http://localhost:8001`   |
| `CHECKER_URL`    | `http://localhost:8002`   |
| `GATEWAY_URL`    | `http://localhost:8000`   |

Docker Compose 環境ではサービス名 (`analytics-api` 等) を DNS 名として解決するため、
`docker-compose.yml` 内で個別の URL が上書きされています。

### 5.3 ロギング

| 変数         | 既定値  | 備考                                                       |
| ------------ | ------- | ---------------------------------------------------------- |
| `LOG_LEVEL`  | `INFO`  | `analytics-api` / `api-gateway` で共通利用 (Go 側は独自制御) |

### 5.4 `analytics-api` 追加設定 (任意)

| 変数                    | 用途                                             |
| ----------------------- | ------------------------------------------------ |
| `MAX_RECORDS`           | 保持するメトリクスレコードの上限                 |
| `METRICS_DEFAULT_LIMIT` | メトリクス取得 API の既定 `limit`                |
| `METRICS_MAX_LIMIT`     | メトリクス取得 API の最大 `limit`                |

### 5.5 `api-gateway` 追加設定 (任意)

| 変数                   | 用途                                                                  |
| ---------------------- | --------------------------------------------------------------------- |
| `PROXY_TIMEOUT`        | 上流サービスへのプロキシ呼び出しタイムアウト (ms)                      |
| `STATUS_PROBE_TIMEOUT` | `/api/status` から `/health` を叩く際のタイムアウト (ms)               |
| `MAX_REQUEST_BODY`     | `express.json` の最大 JSON ボディサイズ                                |
| `SHUTDOWN_TIMEOUT_MS`  | SIGTERM 受信後、進行中リクエスト完了を待つ最大時間 (ms)                |

### 5.6 `health-checker` 追加設定 (任意)

| 変数                         | 用途                                                                      |
| ---------------------------- | ------------------------------------------------------------------------- |
| `CHECK_INTERVAL_SECONDS`     | `>0` でバックグラウンド定期チェックを有効化                              |
| `METRIC_REPORT_MAX_ATTEMPTS` | メトリクス送信失敗時のリトライ回数                                        |
| `METRIC_REPORT_BACKOFF_MS`   | メトリクス送信のバックオフ間隔 (ms)                                       |
| `EXTRA_TARGETS`              | 追加監視対象を JSON 配列で指定 (パース失敗時はデフォルトのみで fail-open) |

## 6. 開発者向けコマンド

主要な操作は `Makefile` にまとまっています。詳細は [`../Makefile`](../Makefile) を
参照してください。

| コマンド         | 内容                                                     |
| ---------------- | -------------------------------------------------------- |
| `make up`        | Docker Compose で全サービスをバックグラウンド起動        |
| `make down`      | 全サービスを停止                                         |
| `make logs`      | Docker Compose のログをフォロー表示                      |
| `make build`     | 各サービスのコンテナイメージをビルド                     |
| `make test`      | Python / Go / TypeScript の全テストを実行                |
| `make lint`      | 各言語のリンタを実行                                     |
| `make clean`     | コンテナ・イメージ・生成物を削除                         |

言語別のテスト・リンタも個別に呼び出せます (`make test-python` / `make test-go` /
`make test-ts` / `make lint-python` / `make lint-go` / `make lint-ts`)。

## 7. 関連ドキュメント

- [`../README.md`](../README.md) — プロジェクト全体像とクイックスタート
- [`../CONTRIBUTING.md`](../CONTRIBUTING.md) — 開発フロー・PR ガイドライン
- [`../SECURITY.md`](../SECURITY.md) — 脆弱性報告手順
- [`../CHANGELOG.md`](../CHANGELOG.md) — 変更履歴
- [`../CODE_OF_CONDUCT.md`](../CODE_OF_CONDUCT.md) — 行動規範
