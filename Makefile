.PHONY: up down logs lint fmt typecheck test test-unit e2e

up:            ## build and start the whole stack
	docker compose up --build -d

down:          ## stop and remove containers + volumes
	docker compose down -v

logs:
	docker compose logs -f api outbox-relay consumer webhook-mock

lint:
	uv run ruff check .
	uv run ruff format --check .

fmt:
	uv run ruff check --fix .
	uv run ruff format .

typecheck:
	uv run mypy

test:          ## unit + integration (integration needs Docker for testcontainers)
	uv run pytest -v

test-unit:
	uv run pytest -v -m "not integration"

e2e:           ## smoke test against a running stack (make up)
	uv run python scripts/e2e.py
