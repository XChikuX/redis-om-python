NAME := aredis_om
SYNC_NAME := redis_om
INSTALL_STAMP := .install.stamp
UV := $(shell command -v uv 2> /dev/null)
REDIS_OM_URL ?= redis://localhost:6380?decode_responses=True
DOCKER_COMPOSE := docker compose
CLUSTER_COMPOSE := $(DOCKER_COMPOSE) -f docker-compose.cluster.yml

.DEFAULT_GOAL := help

.PHONY: help
help:
	@echo "Please use 'make <target>' where <target> is one of"
	@echo ""
	@echo "  install     install packages and prepare environment"
	@echo "  clean       remove all temporary files"
	@echo "  lint        run the code linters"
	@echo "  format      reformat code"
	@echo "  test        run async tests against redis:8-alpine"
	@echo "  test_full   run async + sync tests against redis:8-alpine"
	@echo "  test_oss    run async tests against redis:latest (OSS)"
	@echo "  shell       open a uv shell"
	@echo "  redis       start a Redis instance with Docker"
	@echo "  sync        generate modules redis_om, tests_sync from aredis_om, tests respectively"
	@echo "  dist        build a redis-om package"
	@echo "  upload      upload distributions to PyPI"
	@echo "  all         equivalent to \"make lint format test\""
	@echo ""
	@echo "Check the Makefile to know exactly what each target is doing."

install: $(INSTALL_STAMP)
$(INSTALL_STAMP): pyproject.toml
	@if [ -z $(UV) ]; then echo "uv could not be found. See https://docs.astral.sh/uv/"; exit 2; fi
	$(UV) sync --extra dev
	touch $(INSTALL_STAMP)

.PHONY: clean
clean:
	find . -type d -name "__pycache__" | xargs rm -rf {};
	rm -rf $(INSTALL_STAMP) .coverage .mypy_cache
	rm -rf build
	rm -rf dist
	rm -rf redis_om
	rm -rf tests_sync
	rm -rf .venv
	-$(DOCKER_COMPOSE) down
	-$(CLUSTER_COMPOSE) down


.PHONY: dist
dist: $(INSTALL_STAMP) clean sync
	$(UV) build

.PHONY: sync
sync: $(INSTALL_STAMP)
	$(UV) sync --extra dev
	$(UV) run python make_sync.py
	$(UV) run ruff format $(SYNC_NAME)

.PHONY: lint
lint: $(INSTALL_STAMP) sync
	$(UV) run ruff check ./tests/ $(NAME) $(SYNC_NAME)
	$(UV) run ruff format --check ./tests/ $(NAME) $(SYNC_NAME)
	$(UV) run mypy ./tests/ $(NAME) $(SYNC_NAME) --ignore-missing-imports

.PHONY: format
format: $(INSTALL_STAMP) sync
	$(UV) run ruff check --fix ./tests/ $(NAME) $(SYNC_NAME)
	$(UV) run ruff format ./tests/ $(NAME)

.PHONY: test
test: $(INSTALL_STAMP) sync redis
	REDIS_OM_URL=$(REDIS_OM_URL) $(UV) run pytest -n auto -vv ./tests/ --cov-report term-missing --cov $(NAME)
	$(DOCKER_COMPOSE) down

.PHONY: test_full
test_full: $(INSTALL_STAMP) sync redis
	REDIS_OM_URL=$(REDIS_OM_URL) $(UV) run pytest -n auto -vv ./tests/ ./tests_sync/ --cov-report term-missing --cov $(NAME) $(SYNC_NAME)
	$(DOCKER_COMPOSE) down

.PHONY: test_oss
test_oss: $(INSTALL_STAMP) sync redis
	# Specifically tests against a local OSS Redis instance via
	# docker-compose.yml. Do not use this for CI testing, where we should
	# instead have a matrix of Docker images.
	REDIS_OM_URL=redis://localhost:6381?decode_responses=True $(UV) run pytest -n auto -vv ./tests/ --cov-report term-missing --cov $(NAME)


.PHONY: shell
shell: $(INSTALL_STAMP)
	$(UV) shell

.PHONY: redis
redis:
	$(DOCKER_COMPOSE) up -d

.PHONY: redis_cluster
redis_cluster:
	$(CLUSTER_COMPOSE) up -d
	@echo "Waiting for Redis Cluster nodes to start..."
	@sleep 5
	# Bootstrap via docker exec so we don't need redis-cli on the host.
	# The compose file advertises host.docker.internal (resolves to the
	# Docker host gateway via extra_hosts), so we bootstrap using that
	# address — reachable from inside any container through the published
	# ports. Works on Linux CI, WSL2, and Docker Desktop.
	@if docker exec redis-cluster-7001 redis-cli -h host.docker.internal -p 7001 cluster info 2>/dev/null | grep -q "cluster_state:ok"; then \
		echo "Redis Cluster already bootstrapped."; \
	else \
		echo "Bootstrapping Redis Cluster topology..."; \
		# Retry with backoff in case some nodes aren't ready yet \
		for backoff in 2 4 8 0; do \
			sleep "$$backoff"; \
			if docker exec redis-cluster-7001 redis-cli --cluster create \
				host.docker.internal:7001 host.docker.internal:7002 host.docker.internal:7003 \
				host.docker.internal:7004 host.docker.internal:7005 host.docker.internal:7006 \
				--cluster-replicas 1 --cluster-yes 2>/dev/null; then \
				break; \
			fi; \
			if [ "$$backoff" = "0" ]; then \
				echo "Failed to bootstrap Redis Cluster after retries" >&2; \
				exit 1; \
			fi; \
		done; \
	fi
	@echo "Waiting for Redis Cluster to become healthy..."
	@for attempt in 1 2 3 4 5 6 7 8 9 10 11 12; do \
		if docker exec redis-cluster-7001 redis-cli -h host.docker.internal -p 7001 cluster info 2>/dev/null | grep -q "cluster_state:ok"; then \
			echo "Redis Cluster is healthy."; \
			exit 0; \
		fi; \
		printf "  Waiting for cluster... (attempt $$attempt/12)\n"; \
		sleep 2; \
	done; \
	echo "Redis Cluster did not become healthy in time" >&2; \
	docker exec redis-cluster-7001 redis-cli -h host.docker.internal -p 7001 cluster info 2>&1 || true; \
	exit 1

.PHONY: test_cluster
test_cluster: $(INSTALL_STAMP) sync redis redis_cluster
	REDIS_OM_URL=$(REDIS_OM_URL) $(UV) run pytest -vv ./tests/test_cluster_operations.py --cov-report term-missing --cov $(NAME)
	if [ -e tests_sync/test_cluster_operations.py ]; then \
		REDIS_OM_URL=$(REDIS_OM_URL) $(UV) run pytest -vv ./tests_sync/test_cluster_operations.py --cov-append --cov-report term-missing --cov $(SYNC_NAME); \
	fi
	$(CLUSTER_COMPOSE) down
	$(DOCKER_COMPOSE) down

.PHONY: upload
upload: dist
	$(UV) run twine upload dist/* --verbose

.PHONY: all
all: lint format test
