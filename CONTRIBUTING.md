# Contributing to PulseBoard / コントリビュートガイド

PulseBoard へのコントリビュートに興味を持っていただきありがとうございます。
このドキュメントでは、開発環境の構築からプルリクエスト (PR) 作成までの流れをまとめています。

まずは行動規範 ([`CODE_OF_CONDUCT.md`](./CODE_OF_CONDUCT.md)) をご一読ください。
セキュリティ関連の報告については [`SECURITY.md`](./SECURITY.md) を参照してください。

---

## 目次 / Table of Contents

- [開発環境 / Prerequisites](#開発環境--prerequisites)
- [リポジトリのセットアップ / Setup](#リポジトリのセットアップ--setup)
- [プロジェクト構成 / Project Layout](#プロジェクト構成--project-layout)
- [テスト・Lint の実行 / Running Tests and Linters](#テストlint-の実行--running-tests-and-linters)
- [Docker での起動 / Running with Docker](#docker-での起動--running-with-docker)
- [コーディング規約 / Coding Style](#コーディング規約--coding-style)
- [コミットメッセージ規約 / Commit Message Convention](#コミットメッセージ規約--commit-message-convention)
- [ブランチと Pull Request / Branching and Pull Requests](#ブランチと-pull-request--branching-and-pull-requests)
- [Issue の起票 / Filing Issues](#issue-の起票--filing-issues)

---

## 開発環境 / Prerequisites

PulseBoard は Python / Go / TypeScript の 3 サービスで構成されています。
ローカルで全サービスを扱う場合は以下を用意してください（CI で使用しているバージョンと揃えると安全です）。

| ツール             | 推奨バージョン | 用途                                           |
| ------------------ | -------------- | ---------------------------------------------- |
| Python             | 3.12           | `analytics-api` の実行・テスト                 |
| Go                 | 1.22           | `health-checker` の実行・テスト                |
| Node.js            | 22             | `api-gateway` の実行・テスト                   |
| Docker / Compose   | 最新安定版     | サービス全体を統合起動する場合                 |
| GNU Make           | 任意           | ルート `Makefile` のショートカットを使う場合   |

CI での固定バージョンは [`.github/workflows/ci.yml`](./.github/workflows/ci.yml) を参照してください。

## リポジトリのセットアップ / Setup

```bash
git clone https://github.com/mohadayo/pulseboard.git
cd pulseboard
cp .env.example .env   # 必要に応じて値を編集
```

環境変数の一覧と説明は [`.env.example`](./.env.example) を参照してください。

## プロジェクト構成 / Project Layout

```
pulseboard/
├── analytics-api/    # Python (FastAPI) — メトリクス集計・保存 API
├── health-checker/   # Go — 各サービスへのヘルスチェックとレポート送信
├── api-gateway/      # TypeScript (Express) — 外部向けゲートウェイ
├── docker-compose.yml
├── Makefile          # test / lint / build / up / down 等のショートカット
└── .github/          # CI・Issue/PR テンプレート・CODEOWNERS 等
```

## テスト・Lint の実行 / Running Tests and Linters

ルート `Makefile` に各サービスのテスト・Lint コマンドがまとまっています。

```bash
# すべてのテスト（Python / Go / TypeScript）
make test

# 個別に実行
make test-python
make test-go
make test-ts

# すべての Lint
make lint

# 個別に実行
make lint-python   # flake8 (max-line-length=120)
make lint-go       # go vet ./...
make lint-ts       # eslint src/ --ext .ts
```

サービスディレクトリで直接実行する場合の例:

```bash
# analytics-api
cd analytics-api
pip install -r requirements-dev.txt
pytest -v
flake8 --max-line-length=120 --exclude=__pycache__ main.py

# health-checker
cd health-checker
go vet ./...
go test -v ./...

# api-gateway
cd api-gateway
npm ci
npm run lint
npm test
```

CI と同じコマンドを流していますので、PR 作成前にローカルで `make test` と `make lint` が通ることを確認してください。

## Docker での起動 / Running with Docker

3 サービスをまとめて起動する場合は Docker Compose を使用します。

```bash
make build   # docker compose build
make up      # docker compose up -d
make logs    # docker compose logs -f
make down    # docker compose down
make clean   # ボリューム・ローカルイメージ・生成物を削除
```

各サービスのポートは `.env` で上書きできます（デフォルト: analytics-api 8001 / health-checker 8002 / api-gateway 8000）。

## コーディング規約 / Coding Style

各言語について、CI が検査するツール・設定にあわせてください。

- **Python** (`analytics-api`)
  - `flake8` (`--max-line-length=120`) を通すこと。
  - 型ヒント・docstring を可能な範囲で追加すると尚可。
- **Go** (`health-checker`)
  - `gofmt` / `go vet` に準拠。エディタの保存時整形推奨。
- **TypeScript** (`api-gateway`)
  - [`.eslintrc.json`](./api-gateway/.eslintrc.json) の規約に従う。
  - `tsconfig.json` の設定を尊重し、`any` の乱用を避ける。

エディタ間のインデント差分を防ぐため、EditorConfig 対応エディタの使用を推奨します（ルート `.editorconfig` を参照）。

## コミットメッセージ規約 / Commit Message Convention

[Conventional Commits](https://www.conventionalcommits.org/ja/v1.0.0/) スタイルの日本語プレフィクスを使用してください。

| プレフィクス | 用途                                         |
| ------------ | -------------------------------------------- |
| `feat:`      | 新機能の追加                                 |
| `fix:`       | バグ修正                                     |
| `docs:`      | ドキュメントのみの変更                       |
| `test:`      | テスト追加・修正                             |
| `refactor:`  | 挙動を変えないリファクタリング               |
| `chore:`     | ビルド設定・依存関係更新・雑務               |
| `ci:`        | CI ワークフローの変更                        |

例:

```
feat: health-checker に extra targets の JSON 上書き機能を追加
fix: analytics-api の /metrics で limit=0 のときに 500 になる不具合を修正
```

## ブランチと Pull Request / Branching and Pull Requests

1. `main` から作業ブランチを切ります（例: `feat/extra-targets` / `fix/metrics-limit-zero`）。
2. 変更を実装し、ローカルで `make test` と `make lint` が通ることを確認します。
3. **Draft Pull Request** として PR を作成します（CI がグリーンになるまでは Draft 推奨）。
4. PR 本文は [`.github/PULL_REQUEST_TEMPLATE.md`](./.github/PULL_REQUEST_TEMPLATE.md) のセクション（概要 / 変更内容 / 動作確認手順 / 関連 Issue）を埋めてください。
5. CI (`test-python` / `test-go` / `test-typescript` / `docker-build`) が全てグリーンになったら Draft を解除しレビューを依頼します。
6. マージは **squash merge** を推奨します（コミット履歴を Issue / PR 単位で圧縮）。

対応する Issue がある場合は本文に `Closes #<番号>` を必ず含めてください。

## Issue の起票 / Filing Issues

- バグ報告: [`.github/ISSUE_TEMPLATE/bug_report.md`](./.github/ISSUE_TEMPLATE/bug_report.md) を利用
- 機能要望: [`.github/ISSUE_TEMPLATE/feature_request.md`](./.github/ISSUE_TEMPLATE/feature_request.md) を利用
- セキュリティ脆弱性: 公開 Issue ではなく [`SECURITY.md`](./SECURITY.md) の連絡手段を使用

再現手順・期待挙動・実際の挙動を具体的に記載していただけると助かります。

---

ご協力ありがとうございます! / Thank you for contributing!
