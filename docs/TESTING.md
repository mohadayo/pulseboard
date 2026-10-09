# テスト（TESTING）

PulseBoard は **3 サービス（`analytics-api` / `health-checker` / `api-gateway`）がそれぞれ別のテストランナーで検証される** 構成になっており、ローカル検証と CI (`.github/workflows/ci.yml`) の対応関係・テスト配置ルール・新規追加時の勘所をここに集約します。

Makefile / CI ワークフロー / `CONTRIBUTING.md` に散らばっていた情報の統合インデックスとして参照してください。

## サービスとテストランナー

| サービス | 言語 | テストランナー | テスト配置 | 代表コマンド（Makefile） |
|---|---|---|---|---|
| `analytics-api/` | Python 3.12 | `pytest` | `analytics-api/test_*.py` | `make test-python` |
| `health-checker/` | Go 1.22 | 標準 `go test` | `health-checker/*_test.go` | `make test-go` |
| `api-gateway/` | TypeScript (Node 22) | `jest` | `api-gateway/src/**/*.test.ts` | `make test-ts` |

### Lint / 型検査

| 対象 | コマンド（Makefile） | 内部で実行される内容 |
|---|---|---|
| `analytics-api/` | `make lint-python` | `flake8 --max-line-length=120 --exclude=__pycache__ main.py` |
| `health-checker/` | `make lint-go` | `go vet ./...` |
| `api-gateway/` | `make lint-ts` | `npm run lint`（ESLint）。`node_modules/` 未生成でも成立するよう前段で `npm install --silent` |
| `api-gateway/`（型検査） | `make type-check-ts` | `npm run type-check` → `tsconfig.typecheck.json` を使った `tsc --noEmit` |

> `tsconfig.typecheck.json` が独立しているのは、`tsconfig.json` が build 対象から `**/*.test.ts` を除外しているため、`npm run build` 単独ではテストコードの型エラーを検出できないため。CI が `npm test` の前段で `npm run type-check` を噛ませているのはこの事情に対応する。

## ローカルで CI と等価な検証を行う

CI (`.github/workflows/ci.yml`) は以下 4 ジョブで構成される：

| ジョブ | 実行順 | 内部ステップ |
|---|---|---|
| `test-python` | 並列 | `flake8 → pytest` |
| `test-go` | 並列 | `go vet → go test` |
| `test-typescript` | 並列 | `npm ci → npm run lint → npm run type-check → npm test` |
| `docker-build` | 直列（上記 3 本に `needs`） | `docker compose build` |

これらをローカルで一括再現するには Makefile の集約ターゲットを使う：

```sh
make ci
```

`make ci` は内部で `lint → type-check → test` の順に実行し、途中で失敗すると Make のデフォルト挙動により後続はスキップされます。CI の順序と一致しているため、push 前の最終確認として走らせると CI 失敗の多くを先取り検知できます。

個別実行の例：

```sh
# 分析 API だけテストしたい
make test-python

# Go だけフォーマット・テスト
make lint-go test-go

# TypeScript 側の型エラーだけ早期検出したい
make type-check-ts
```

## 新しいテストを追加する時のチェックリスト

### Python (`analytics-api/`)

- [ ] `analytics-api/test_<対象モジュール>.py` にファイルを作る（`pytest` のデフォルト discovery に合わせる）
- [ ] テスト用の依存は `requirements-dev.txt` に追加する（本番 `requirements.txt` には入れない）
- [ ] `flake8 --max-line-length=120` の対象から外れないよう、新規ファイルを `main.py` 相当として扱わない場合も PEP8 に合わせる
- [ ] 外部 I/O（HTTP / DB / ファイル）はモック化し、CI での flakiness を避ける

### Go (`health-checker/`)

- [ ] テストファイル名は `<対象>_test.go`、関数は `TestXxx(t *testing.T)` の規約に従う
- [ ] テーブル駆動テストを基本とし、`t.Run(name, ...)` でサブテスト名を付ける
- [ ] `go vet ./...` を通す（`make lint-go` と同じ）。ベクタ化できるなら `go test -race ./...` もローカルで一度は走らせる

### TypeScript (`api-gateway/`)

- [ ] `api-gateway/src/<対象>/*.test.ts` に配置する（`jest` のデフォルト設定）
- [ ] `tsconfig.typecheck.json` の include に該当することを確認する（上記「tsconfig の分割」節を参照）
- [ ] モックには `jest.mock(...)` を使い、同じモジュールを import する他のテストに副作用が漏れないよう `jest.resetModules()` / `beforeEach` でリセットする
- [ ] ESLint の警告はエラーと同等に扱う（CI の `npm run lint` は warning もあれば `--max-warnings 0` 相当で落ちる前提）

### 共通

- [ ] CI の 3 ジョブ (`test-python` / `test-go` / `test-typescript`) すべてが新規テストで緑であることを `make ci` でローカル確認してから push する
- [ ] テストが外部サービス（Redis / PostgreSQL 等）に依存する場合は `docker-compose.yml` の構成とも整合させ、不要にネットワークを張らない

## タイムアウトとキャンセル挙動

- CI の各ジョブは `timeout-minutes: 10`（docker-build のみ 20）で打ち切られる。これより長く走るテストは **設計を見直す**（ユニットでなく統合テスト or e2e は別パイプラインを検討）。
- 同一 ref に短時間で連続 push した場合は、CI の `concurrency` 設定により進行中の古いジョブがキャンセルされる。ローカルの `make ci` にはこの挙動はないため、集中的に走らせたい時は手で中断する。

## 関連ドキュメント

- [`../CONTRIBUTING.md`](../CONTRIBUTING.md) — ブランチ運用・コミット規則・レビューの流れ
- [`./ARCHITECTURE.md`](./ARCHITECTURE.md) — 3 サービスの責務とサービス間通信（テストの境界設計を考える時の参照先）
- [`./TROUBLESHOOTING.md`](./TROUBLESHOOTING.md) — テスト以外の運用で発生しがちな事象の切り分け
- [`../Makefile`](../Makefile) — 本ドキュメントが参照する全ターゲットの一次定義
- [`../.github/workflows/ci.yml`](../.github/workflows/ci.yml) — CI の一次定義（ジョブ構成・ステップ順序）
