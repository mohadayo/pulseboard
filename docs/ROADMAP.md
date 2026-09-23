# PulseBoard ロードマップ

本ドキュメントは PulseBoard がどの方向に進もうとしているか、直近何に取り組んでいるかを、コードや Issue / PR 一覧を読み解かなくても把握できるようにするための場所です。

- **対象読者**: 新規コントリビュータ、たまに触りに来る開発者、ステークホルダー
- **更新頻度**: 各フェーズの内容に大きな進捗・方針変更があったタイミング（毎リリース必須ではない）
- **粒度**: 個別 Issue / PR ではなく「領域単位（テーマ）」で書く。個別タスクは Issue にリンクする

> ロードマップは約束ではなく、現時点での意図・優先順位のスナップショットです。実装順序や採用可否は Issue / PR 上のレビューで最終決定します。

## プロダクトビジョン

PulseBoard は **「複数マイクロサービスの死活・応答性能を、少ないセットアップでまとめて可視化する」** ためのミニマルな監視基盤です。

- 対象: 数個〜数十個のサービスを内製している小〜中規模チーム
- 提供価値:
  - **1 コマンドで起動できる**（Docker Compose）
  - **サービス横断のサマリと個別サービスの詳細を同じ API 経路で取れる**（API Gateway 経由）
  - **多言語スタック（Python / Go / TypeScript）の実践的な参考実装** としても機能する

## 現在地（Snapshot）

- 3 サービス（`analytics-api` / `health-checker` / `api-gateway`）による最小構成が稼働
- サービス横断・単一サービス双方の集計 API が揃い、時系列・インシデント・SLA (uptime / MTTR) 集約まで対応
- Docker Compose 一発起動、GitHub Actions で 3 言語すべての lint / test / build が回る
- ドキュメント: [`ARCHITECTURE`](./ARCHITECTURE.md) / [`DEPLOYMENT`](./DEPLOYMENT.md) / [`FAQ`](./FAQ.md) / [`OBSERVABILITY`](./OBSERVABILITY.md) / [`RUNBOOK`](./RUNBOOK.md) / [`TROUBLESHOOTING`](./TROUBLESHOOTING.md) が整備済み

## Now — 直近取り組んでいる領域

**継続的に PR が動いており、レビュー・改善の優先度が高い領域。**

- **API 網羅性の底上げ**
  - `/metrics/*` 系エンドポイントの拡充（overview / count / timeseries / incidents / uptime など）
  - API Gateway 側での proxy 経路の追随、README の API テーブルの整合維持
- **可観測性の底上げ**
  - 構造化ログ・リクエスト ID 伝播・ヘルスチェック経路の明確化
  - [`OBSERVABILITY.md`](./OBSERVABILITY.md) を軸に、実運用で必要な観測点をドキュメントとコード両面で整える
- **堅牢化・小さな運用改善**
  - Dockerfile の非 root ユーザ化、マルチステージ化、`.dockerignore` 整備
  - タイムアウト・リトライ・graceful shutdown 系設定を環境変数で外出し
  - 入力バリデーション（`q` 文字数上限、URL スキーム検証、`MAX_RECORDS` floor guard など）
- **開発者体験 (DX)**
  - `Makefile` レシピの整備、CI での型検査追加、Dockerfile 静的解析（hadolint）
  - コントリビュート導線の明文化（[`CONTRIBUTING.md`](../CONTRIBUTING.md) / [`SUPPORT.md`](../.github/SUPPORT.md) / [`SECURITY.md`](../SECURITY.md)）

## Next — 近いうちに着手予定の領域

**現時点ではまだ着手していないが、Now 完了後・並行して取り組みたい領域。優先順位付きではなく候補セット。**

- **永続化バックエンド**
  - 現在の in-memory ストアから、任意でファイル / SQLite / 外部 DB へ切り替え可能なストアインターフェース化
  - 既存 API の互換性を保ちつつ、`MAX_RECORDS` によるリング状 evict の挙動を移行先でも再現可能にする
- **認証・認可**
  - まずは API Gateway 側での API Key / Basic 認証など軽量な仕組みから
  - サービス間通信（Gateway → analytics-api / health-checker）の共有シークレット導入
- **アラート / 通知**
  - `unhealthy` の継続時間や uptime SLA 割れをトリガに、Webhook / Slack / メール等に通知する経路
  - 既存の `incidents` / `uptime` エンドポイントで観測できる状態を通知にマッピングする
- **設定バリデーションの強化**
  - 起動時にすべての環境変数を検証し、不整合があれば `fail-fast` する（現在は fail-open な箇所もある）
- **ドキュメント整備**
  - [`docs/GLOSSARY.md`](./GLOSSARY.md)（用語集: `uptime_pct` / `MTTR` / `incident` / `bucket_seconds` など）
  - サービス単位の README（`analytics-api/README.md` など）でオーナーシップを明確化

## Later — 構想段階の領域

**取り組む可能性はあるが、Now / Next が片付いてから検討する領域。合意形成前の段階。**

- **軽量な UI ダッシュボード**
  - 現状は API のみ。最小の Web UI（サービス一覧・時系列チャート・インシデント一覧）を同居させるか、別リポジトリに切り出すか自体が未決定
- **マルチテナント / チーム分離**
  - 複数チーム・複数環境の観測データを 1 インスタンスで分離管理する仕組み
- **プッシュ型メトリクス受信**
  - 現状の pull 型ヘルスチェックに加え、Prometheus remote_write / OpenTelemetry 互換の受信口
- **プラグイン / 拡張ポイント**
  - `EXTRA_TARGETS` のような JSON 構成だけでなく、Go プラグイン・スクリプト差し込み等でチェック種別を拡張

## 非目標 (Non-Goals)

**誤解を避けるため、意図的に「やらない」領域を明文化する。**

- **フルスタックの APM / トレーシング基盤の代替にはならない**
  - PulseBoard は「死活と応答性能の集約」に絞ったスコープを維持する
  - 分散トレーシングは [`OBSERVABILITY.md`](./OBSERVABILITY.md) が示す通り、Jaeger / Tempo / Datadog 等の既存基盤と接続する立場をとる
- **時系列 DB の代替にはならない**
  - ログ・メトリクスの長期保管は Prometheus / VictoriaMetrics / ClickHouse 等の専用基盤に委ねる
  - `MAX_RECORDS` によるリング状 evict は「直近状態の高速参照」用途に最適化する
- **本番グレードのマルチテナント SaaS 化は現時点ではスコープ外**
- **UI 依存の機能を API より優先することはしない**
  - API-first であり、UI は API を通じてのみデータを見る立場をとる

## コントリビュートの入口

「何を優先してレビューしたいか」の目安として:

| フェーズ | 歓迎される PR の種類 |
|---------|---------------------|
| Now | 既存エンドポイントの網羅性向上 / バグ修正 / テスト追加 / ドキュメント整備 / CI・Docker 改善 |
| Next | 大きめの構造変更（永続化・認証など）は事前に Issue で設計を握ってから PR を送る |
| Later | Discussions か Issue で「こういう方向で議論したい」から始める（いきなり大 PR は避ける） |

コントリビュート時の一般的な手順は [`CONTRIBUTING.md`](../CONTRIBUTING.md) を参照してください。セキュリティ関連の報告は [`SECURITY.md`](../SECURITY.md) に従って非公開ルートで連絡します。

## ロードマップ更新のガイド

- 大きな方針変更（例: Later → Next に昇格 / スコープから外す）が発生したら、本ドキュメントを PR で更新する
- 個別タスクの粒度で書きすぎない。個別タスクは Issue で管理し、ここではテーマ単位でリンクにとどめる
- 「完了した項目を Done セクションに残す」より、[`CHANGELOG.md`](../CHANGELOG.md) 側にリリース単位で残す運用を優先する（本ドキュメントはあくまで「これから」の視点）
