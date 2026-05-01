.PHONY: test test-python test-go test-ts lint up down build clean

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

lint-ts:
	cd api-gateway && npm run lint

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
