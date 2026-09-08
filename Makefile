.PHONY: test test-python test-go test-ts lint lint-python lint-go lint-ts type-check type-check-ts ci up down build clean

test: test-python test-go test-ts

test-python:
	cd analytics-api && pip install -q -r requirements-dev.txt && pytest -v

test-go:
	cd health-checker && go test -v ./...

test-ts:
	cd api-gateway && npm install --silent && npm test

lint: lint-python lint-go lint-ts

lint-python:
	cd analytics-api && pip install -q -r requirements-dev.txt && flake8 --max-line-length=120 --exclude=__pycache__ main.py

lint-go:
	cd health-checker && go vet ./...

# 旧実装は `npm install` を挟まず `npm run lint` だけを実行していたため、
# `node_modules/` が未生成のリポジトリで `make lint` を先に呼ぶと eslint
# 本体が見つからず失敗していた（test-ts / type-check-ts は install 済み）。
# 兄弟ターゲットと同じ前処理に揃え、どの順で叩いても成立するようにする。
lint-ts:
	cd api-gateway && npm install --silent && npm run lint

# tsc は tsconfig.json 側でテストファイルを除外しているため、`npm run build`
# だけではテストコードの型エラーが検出できない。CI と一致させるため、
# tsconfig.typecheck.json を用いてテストコードも含めた `tsc --noEmit` を
# ローカルからも簡単に走らせられるショートカットを提供する。
type-check: type-check-ts

type-check-ts:
	cd api-gateway && npm install --silent && npm run type-check

# CI パイプライン (.github/workflows/ci.yml) をローカルで再現する集約ターゲット。
# CI と同じ順序 (lint → type-check → test) で走らせるため、push 前にこの
# 1 コマンドで CI 失敗を先取り検知できる。個別ターゲットが失敗した時点で
# Make のデフォルト挙動により後続はスキップされる。
ci: lint type-check test

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

clean:
	docker compose down -v --rmi local
	rm -rf api-gateway/node_modules api-gateway/dist
	rm -rf analytics-api/__pycache__
	rm -f health-checker/health-checker
