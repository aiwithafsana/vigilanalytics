# ── Vigil — developer convenience targets ────────────────────────────────────
# Usage:  make <target>
# All docker compose commands operate from infra/docker-compose.yml.

COMPOSE      := docker compose -f infra/docker-compose.yml
COMPOSE_ML   := $(COMPOSE) --profile ml
BACKEND_RUN  := $(COMPOSE) run --rm backend
ML_RUN       := $(COMPOSE_ML) run --rm ml

.PHONY: help dev build stop clean logs \
        migrate seed psql \
        test lint \
        pipeline pipeline-fast leie-refresh \
        secret fmt

# ── Stack lifecycle ───────────────────────────────────────────────────────────

dev: ## Start full stack in development mode (hot-reload, logs to stdout)
	$(COMPOSE) up --build

build: ## Build (or rebuild) all service images
	$(COMPOSE) build

stop: ## Stop all running services
	$(COMPOSE) down

clean: ## Stop services and DESTROY all volumes (WARNING: data loss)
	$(COMPOSE) down -v --remove-orphans

logs: ## Tail logs from all services
	$(COMPOSE) logs -f

logs-%: ## Tail logs from a specific service  e.g. make logs-backend
	$(COMPOSE) logs -f $*

# ── Database ──────────────────────────────────────────────────────────────────

migrate: ## Run pending Alembic migrations
	$(BACKEND_RUN) alembic upgrade head

migrate-rollback: ## Roll back the last Alembic migration
	$(BACKEND_RUN) alembic downgrade -1

migrate-history: ## Show Alembic migration history
	$(BACKEND_RUN) alembic history

seed: ## Seed the database with demo providers and users
	$(BACKEND_RUN) python -m db.seed

psql: ## Open a psql shell in the database container
	$(COMPOSE) exec db psql -U vigil -d vigil

# ── Quality ───────────────────────────────────────────────────────────────────

test: ## Run backend pytest suite
	$(BACKEND_RUN) pytest tests/ -v --tb=short --asyncio-mode=auto

lint: ## Lint backend with ruff
	$(BACKEND_RUN) ruff check app/

fmt: ## Auto-format backend with ruff
	$(BACKEND_RUN) ruff check app/ --fix

# ── ML pipeline ───────────────────────────────────────────────────────────────

pipeline: ## Run full ML pipeline (ingest + train + score + load DB)
	$(ML_RUN)

pipeline-fast: ## Score + load DB using cached data and saved models (no re-download)
	$(ML_RUN) --skip-ingest --skip-train

pipeline-dry: ## Full pipeline dry-run (stop before writing to DB)
	$(ML_RUN) --dry-run

leie-refresh: ## Refresh LEIE exclusion list only (fast, ~30s)
	$(ML_RUN) --leie-only

# ── Utilities ─────────────────────────────────────────────────────────────────

secret: ## Generate a secure SECRET_KEY (copy output to .env)
	@openssl rand -hex 32

# ── Help ──────────────────────────────────────────────────────────────────────

help: ## Show this help message
	@printf "\n\033[1mVigil — available make targets\033[0m\n\n"
	@grep -E '^[a-zA-Z_%/-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@printf "\n"

.DEFAULT_GOAL := help
