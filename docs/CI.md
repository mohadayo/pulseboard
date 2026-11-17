# CI / CD

PulseBoard の GitHub Actions ワークフローを **横断的に俯瞰する一次情報**。`.github/workflows/` には 5 本のワークフローがあり、それぞれ目的・発火契機・権限・ローカル再現手段が異なります。本ドキュメントはそれらを 1 枚で一覧できるようにまとめ、各ワークフローの深堀りは該当 YAML の先頭コメント（および [TESTING.md](./TESTING.md)）に委譲します。

## ワークフロー一覧

| ワークフロー | 目的 | 主な発火契機 | 代表 permissions | timeout（分） |
|---|---|---|---|---|
| [`ci.yml`](../.github/workflows/ci.yml) | 3 サービスのテスト・Lint・型検査 + docker build | `push: [main]`, `pull_request: [main]` | `contents: read` | 各 10（docker-build のみ 20） |
| [`actionlint.yml`](../.github/workflows/actionlint.yml) | `.github/workflows/*.yml` の静的解析（actionlint + shellcheck） | 上記 + `workflow_dispatch`（`.github/workflows/**` paths フィルタ） | `contents: read` | 5 |
| [`codeql.yml`](../.github/workflows/codeql.yml) | Python / TypeScript / Go の SAST（`security-extended` クエリ） | 上記 + `schedule: '0 3 * * 0'`（週次） | `contents: read`, `security-events: write`, `actions: read` | 20 |
| [`dependency-review.yml`](../.github/workflows/dependency-review.yml) | PR で追加された依存の CVE / ライセンス審査 | `pull_request: [main]` のみ | `contents: read`, `pull-requests: write` | 5 |
| [`hadolint.yml`](../.github/workflows/hadolint.yml) | 3 サービス Dockerfile の静的解析（hadolint） | 上記 + `workflow_dispatch`（Dockerfile / `.hadolint.yaml` paths フィルタ） | `contents: read` | 5 |

> すべてのワークフローは `concurrency.cancel-in-progress: true` を設定しており、同一 ref への短時間連続 push では進行中の古いジョブがキャンセルされます。

## 役割分担

5 本のワークフローは **意図的に責務を分離** しています。どの CI が落ちたかで原因領域を切り分けられる設計です。

- `ci.yml` — アプリケーションコードの動作保証（テスト・Lint・型検査）。失敗 = 機能的なデグレ。
- `actionlint.yml` — CI 設定ファイル自身の静的解析。失敗 = ワークフロー YAML の記述ミス。
- `codeql.yml` — ソースコードの脆弱性検出（CWE: SQLi / XSS / Path Traversal 等）。失敗 = セキュリティリスク。
- `dependency-review.yml` — 依存パッケージ経由の CVE とライセンスリスク。失敗 = 外部依存の既知脆弱性または非許容ライセンス（AGPL-3.0, GPL-2.0, GPL-3.0）。
- `hadolint.yml` — Dockerfile のベストプラクティス違反（未 pin ベースイメージ・root 実行・レイヤ順序など）。失敗 = コンテナイメージの品質低下。

## 発火契機の違い

| トリガ | ci.yml | actionlint | codeql | dependency-review | hadolint |
|---|---|---|---|---|---|
| `push: [main]` | ✅ | ✅（paths 絞り） | ✅ | — | ✅（paths 絞り） |
| `pull_request: [main]` | ✅ | ✅（paths 絞り） | ✅ | ✅ | ✅（paths 絞り） |
| `schedule` | — | — | ✅（日曜 03:00 UTC） | — | — |
| `workflow_dispatch` | — | ✅ | ✅ | — | ✅ |

> `dependency-review` は push では走りません（差分ベース解析のため PR 限定）。

## 権限（`permissions:`）の方針

全ワークフローが **least-privilege** 原則に従います。

- ベースラインは `contents: read` のみ。
- 書き込み権限は必要なジョブにのみ、必要な種類だけ付与:
  - `codeql.yml` → `security-events: write`（Security タブへの結果書き込み）、`actions: read`（自ワークフローのメタデータ取得）
  - `dependency-review.yml` → `pull-requests: write`（`comment-summary-in-pr: on-failure` の投稿先）
- 将来ジョブ単位で追加権限が必要になった場合は、ワークフロー全体ではなく **ジョブ単位の `permissions:`** で上書きする運用（`ci.yml` の先頭コメント参照）。

参考: [GitHub Docs — Automatic token authentication](https://docs.github.com/en/actions/security-guides/automatic-token-authentication)

## ローカルでの再現手順

### `ci.yml` 相当

Makefile の集約ターゲットが最短経路です。

```sh
make ci     # lint → type-check → test を CI と同順で実行
make build  # ci.yml の docker-build ジョブ相当
```

個別実行・テスト運用の詳細は [TESTING.md](./TESTING.md) を参照してください。

### `actionlint.yml` 相当

公式配布のバイナリを `scripts/download-actionlint.bash` で取得して実行します（CI と同じ方式）。

```sh
# ワークスペース直下に actionlint バイナリを落とす（CI と同じ v1.7.7）
bash <(curl -sSL https://raw.githubusercontent.com/rhysd/actionlint/v1.7.7/scripts/download-actionlint.bash) 1.7.7
./actionlint -color
```

### `codeql.yml` 相当

CodeQL CLI（`codeql` コマンド）のセットアップが必要で、ローカル実行はコストが高めです。基本は PR / schedule 実行での検知に任せ、どうしてもローカル確認が必要な場合のみ [GitHub Docs — Running CodeQL CLI](https://docs.github.com/en/code-security/codeql-cli) を参照してください。

### `dependency-review.yml` 相当

本ワークフローは GitHub Advisory DB への差分照合が本体のため、ローカル完全再現はできません。ローカルで類似チェックを行うなら:

- Python: `pip-audit -r analytics-api/requirements.txt`
- Node: `npm --prefix api-gateway audit`
- Go: `cd health-checker && go list -json -m all | govulncheck ./...`

### `hadolint.yml` 相当

CI と同じポリシー（`.hadolint.yaml`）・同じしきい値（`failure-threshold: warning`）でローカル実行できます。

```sh
# Docker 経由（インストール不要）
docker run --rm -i -v "$PWD/.hadolint.yaml:/.config/hadolint.yaml" \
  hadolint/hadolint hadolint - < analytics-api/Dockerfile
docker run --rm -i -v "$PWD/.hadolint.yaml:/.config/hadolint.yaml" \
  hadolint/hadolint hadolint - < api-gateway/Dockerfile
docker run --rm -i -v "$PWD/.hadolint.yaml:/.config/hadolint.yaml" \
  hadolint/hadolint hadolint - < health-checker/Dockerfile

# または直接バイナリ（Homebrew: `brew install hadolint`）
hadolint --config .hadolint.yaml **/Dockerfile
```

## 既存ドキュメントとの関係

- [TESTING.md](./TESTING.md) — **テスト観点の一次情報**。`ci.yml` の test 系ジョブ（`test-python` / `test-go` / `test-typescript`）とローカル再現、新規テスト追加のチェックリストはこちら。本 CI.md は「CI 全体の index」として、テスト詳細は TESTING.md へ委譲しています。
- [DEPLOYMENT.md](./DEPLOYMENT.md) — 本番デプロイ側の CI/CD（将来的に追加されるリリースワークフロー等）。現状はローカル / Docker Compose 中心の手順。
- [`../CONTRIBUTING.md`](../CONTRIBUTING.md) — ブランチ運用・コミット規則・レビューの流れ。
- [`../.hadolint.yaml`](../.hadolint.yaml) — hadolint の共有ポリシー。
- [`../Makefile`](../Makefile) — `make ci` / `make build` など CI 相当のローカルショートカット。

## ワークフローを追加・改修する時のガイド

- 新規ワークフローを追加した場合は、本ファイルの「ワークフロー一覧」「発火契機の違い」「ローカルでの再現手順」の 3 セクションにエントリを追加してください（行追加が最小単位）。
- 権限を上げる変更（`contents: write` / `pull-requests: write` 等）は、ワークフロー全体ではなく該当ジョブに限定する `permissions:` 上書きを優先してください。理由は本 CI.md の「権限の方針」に揃えます。
- `timeout-minutes` を既定値のまま運用するのは避け、各ジョブの想定実行時間 + 若干のマージンを明示してください（CI の暴走でランナー時間を浪費しないため）。
- `paths` フィルタを設定している既存ワークフローに新規トリガパスを加える場合は、関連するファイル（`actionlint.yml` なら `.github/workflows/**`、`hadolint.yml` なら `**/Dockerfile`）の範囲が揺らいでいないかを本ファイルの表で確認してください。
