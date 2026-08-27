# オブザーバビリティ運用ガイド

`pulseboard` のマイクロサービス群 (analytics-api / api-gateway / health-checker) を安定運用するための、メトリクス・ログ・トレースの三本柱に関する統一的な運用方針をまとめる。

障害対応の具体的な手順は `docs/TROUBLESHOOTING.md` を参照する。全体のシステム構成は `docs/ARCHITECTURE.md` を参照する。

## 目次

- [1. 基本方針](#1-基本方針)
- [2. サービス別 主要メトリクス](#2-サービス別-主要メトリクス)
- [3. 構造化ログ](#3-構造化ログ)
- [4. 分散トレース](#4-分散トレース)
- [5. SLO / SLI とエラーバジェット](#5-slo--sli-とエラーバジェット)
- [6. アラート設計](#6-アラート設計)
- [7. 障害対応時の観測手順](#7-障害対応時の観測手順)

## 1. 基本方針

- **三本柱を明確に分離する**: メトリクスは集計値、ログは離散イベント、トレースはリクエスト単位の因果関係を扱う。同じ情報を複数のシステムで冗長に持たない。
- **プル型モニタリング**: メトリクスは Prometheus 形式で `/metrics` エンドポイントを各サービスが公開し、外部の収集基盤がスクレイプする。
- **ログは stdout / stderr へ**: コンテナ環境では標準出力に JSON 行として出力し、コレクター側で集約する。ファイルにローテートしない。
- **相関 ID の伝播**: すべてのサービス間通信で `X-Request-Id` を透過的に受け渡し、ログ・トレースにも同じ ID を含める。

## 2. サービス別 主要メトリクス

すべてのサービスは以下を最低限公開する。

| 名前 | 種類 | 説明 |
| :-- | :-- | :-- |
| `http_requests_total` | Counter | ステータスコード・パス別のリクエスト総数 |
| `http_request_duration_seconds` | Histogram | リクエストのレイテンシ分布 |
| `process_start_time_seconds` | Gauge | プロセス起動時刻 (稼働時間の算出に使用) |
| `build_info` | Gauge | ビルドバージョン (label) を保持する定数 1 |

### analytics-api (Python)

- `analytics_ingest_events_total`: 取り込みイベント数 (ソース別ラベル)
- `analytics_query_duration_seconds`: 集計クエリのレイテンシ
- `analytics_backlog_size`: 未処理キューの深さ

### api-gateway (TypeScript / Node.js)

- `gateway_upstream_errors_total`: 上流サービス別のエラー数
- `gateway_ratelimit_rejected_total`: レート制限で拒否したリクエスト数
- `gateway_active_connections`: アクティブなコネクション数

### health-checker (Go)

- `healthcheck_probe_duration_seconds`: プローブ実行時間 (対象別ラベル)
- `healthcheck_probe_success_total` / `_failure_total`: 成功・失敗回数
- `healthcheck_consecutive_failures`: 連続失敗回数 (アラート判定に使用)

## 3. 構造化ログ

すべてのログ行は 1 行 1 JSON とし、次のフィールドを必須で含める。

| フィールド | 型 | 説明 |
| :-- | :-- | :-- |
| `ts` | string (RFC3339) | イベント発生時刻 |
| `level` | string | `debug` / `info` / `warn` / `error` |
| `service` | string | サービス名 (`analytics-api` など) |
| `msg` | string | 人間可読なメッセージ |
| `request_id` | string | 相関 ID (該当時) |
| `error` | string | エラーメッセージ (level=error 時) |

### 禁則事項

- パスワード・アクセストークン・PII (氏名・メール・電話番号) を平文で出力しない。マスキング関数を通す。
- `error` レベル以外でスタックトレースを出さない。
- 1 リクエストあたりのログ行は 5 行以内を目安とする (デバッグを除く)。

## 4. 分散トレース

- OpenTelemetry SDK でスパンを送出し、コレクターが集約する。
- サンプリング率は環境変数 `OTEL_TRACES_SAMPLER_ARG` で調整する。本番のデフォルトは 10%、staging は 100%。
- スパン名はサービス境界で命名する: `HTTP GET /api/v1/metrics`, `db.query analytics_events` など。カーディナリティの高い ID をスパン名に埋め込まない (attribute として持たせる)。

## 5. SLO / SLI とエラーバジェット

| サービス | SLI | 目標 SLO | エラーバジェット |
| :-- | :-- | :-- | :-- |
| api-gateway | 5xx 率 | 30 日で 99.9% 未満 | 43 分 12 秒 / 30 日 |
| api-gateway | P95 レイテンシ | 30 日で 300 ms 以下 | ── |
| analytics-api | 集計ジョブ成功率 | 7 日で 99.5% | 50 分 24 秒 / 7 日 |
| health-checker | プローブ実行遅延 | 5 分周期に対して P99 で 10 秒以内 | ── |

エラーバジェットを 30 日で 50% 消費した時点で、新機能開発を停止し信頼性改善に振り向ける判断基準とする。

## 6. アラート設計

アラートは "現在ユーザーに影響が出ている / 出そうな事象" のみを鳴らす。ノイズの多いアラートは即座に閾値を見直すか削除する。

### 通知先ポリシー

| 深刻度 | 通知先 | 応答目標 |
| :-- | :-- | :-- |
| Critical | オンコール PagerDuty | 5 分以内に一次対応 |
| Warning | Slack `#pulseboard-alerts` | 翌営業日中 |
| Info | 通知しない (ダッシュボードのみ) | ── |

### 主要アラートルール例

- `rate(http_requests_total{status=~"5.."}[5m]) / rate(http_requests_total[5m]) > 0.02` (5 分平均 5xx 率 2%): Warning
- 上記が 15 分継続、または 5% 超過: Critical
- `healthcheck_consecutive_failures >= 3`: Critical
- `up{job="analytics-api"} == 0` が 2 分継続: Critical

## 7. 障害対応時の観測手順

1. Slack `#pulseboard-alerts` および PagerDuty で受信したアラートの対象サービスを確認する。
2. ダッシュボードでゴールデンシグナル (レイテンシ / トラフィック / エラー / サチュレーション) を横並びに確認し、影響範囲を切り分ける。
3. 該当時刻の `request_id` を取得し、ログ・トレースを横断して原因を特定する。
4. 一次対応後、`docs/TROUBLESHOOTING.md` に手順を追記する。
5. 事後、ポストモーテムを作成しエラーバジェットへの影響を記録する。

## 変更履歴

- 2026-08: 初版作成。
