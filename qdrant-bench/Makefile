# Top-level orchestration. All commands assume you're at the repo root.

SHELL := /bin/bash

# ---- Cluster lifecycle ----------------------------------------------------

DOCKER_DIR := deploy
COMPOSE_3 := -f $(DOCKER_DIR)/docker-compose.3node.yml
COMPOSE_5 := -f $(DOCKER_DIR)/docker-compose.5node.yml

# gRPC ports — must match host-side port mappings in the compose files
HOSTS_3 := localhost:6334,localhost:6434,localhost:6534
HOSTS_5 := localhost:6334,localhost:6434,localhost:6534,localhost:6634,localhost:6734

# REST URLs — used by chaos and cluster-status targets
REST_3 := http://localhost:6333,http://localhost:6433,http://localhost:6533
REST_5 := http://localhost:6333,http://localhost:6433,http://localhost:6533,http://localhost:6633,http://localhost:6733

# Container names — must match container_name in the compose files
CONTAINERS_3 := qdrant_node1,qdrant_node2,qdrant_node3
CONTAINERS_5 := qdrant_node1,qdrant_node2,qdrant_node3,qdrant_node4,qdrant_node5

.PHONY: cluster-up-3 cluster-up-5 cluster-down cluster-down-keep cluster-status cluster-status-5 cluster-logs cluster-logs-5

cluster-up-3: ## bring up the 3-node Qdrant cluster
	@echo "[make] starting 3-node Qdrant cluster..."
	mkdir -p $(DOCKER_DIR)/data/node1 $(DOCKER_DIR)/data/node2 $(DOCKER_DIR)/data/node3 \
	         $(DOCKER_DIR)/snapshots/node1 $(DOCKER_DIR)/snapshots/node2 $(DOCKER_DIR)/snapshots/node3
	docker compose $(COMPOSE_3) up -d
	@echo "[make] waiting for nodes to become ready..."
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
	  ok=$$(curl -sf http://localhost:6333/cluster | grep -c '"status":"enabled"' || true); \
	  if [ "$$ok" = "1" ]; then echo "[make] cluster ready"; exit 0; fi; \
	  echo "[make] waiting ($$i/10)..."; sleep 3; \
	done; echo "[make] cluster did not come up"; exit 1

cluster-up-5: ## bring up the 5-node Qdrant cluster
	@echo "[make] starting 5-node Qdrant cluster..."
	mkdir -p $(DOCKER_DIR)/data5/node1 $(DOCKER_DIR)/data5/node2 $(DOCKER_DIR)/data5/node3 $(DOCKER_DIR)/data5/node4 $(DOCKER_DIR)/data5/node5 \
	         $(DOCKER_DIR)/snapshots5/node1 $(DOCKER_DIR)/snapshots5/node2 $(DOCKER_DIR)/snapshots5/node3 $(DOCKER_DIR)/snapshots5/node4 $(DOCKER_DIR)/snapshots5/node5
	docker compose $(COMPOSE_5) up -d
	@echo "[make] waiting for nodes to become ready..."
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
	  ok=$$(curl -sf http://localhost:6333/cluster | grep -c '"status":"enabled"' || true); \
	  if [ "$$ok" = "1" ]; then echo "[make] cluster ready"; exit 0; fi; \
	  echo "[make] waiting ($$i/10)..."; sleep 3; \
	done; echo "[make] cluster did not come up"; exit 1

cluster-down: ## stop and remove cluster (and its data)
	-docker compose $(COMPOSE_3) down -v
	-docker compose $(COMPOSE_5) down -v

cluster-down-keep: ## stop containers but keep volumes (chaos rerun friendly)
	-docker compose $(COMPOSE_3) stop
	-docker compose $(COMPOSE_5) stop

cluster-status: ## show /cluster on each 3-node node
	@for p in 6333 6433 6533; do \
	  echo "=== :$$p ==="; \
	  curl -s http://localhost:$$p/cluster | python3 -m json.tool || true; \
	done

cluster-status-5: ## show /cluster on all 5 nodes
	@for p in 6333 6433 6533 6633 6733; do \
	  echo "=== :$$p ==="; \
	  curl -s http://localhost:$$p/cluster | python3 -m json.tool || true; \
	done

cluster-logs: ## tail logs from 3-node cluster
	docker compose $(COMPOSE_3) logs -f --tail 100

cluster-logs-5: ## tail logs from 5-node cluster
	docker compose $(COMPOSE_5) logs -f --tail 100

# ---- Dataset --------------------------------------------------------------

.PHONY: data
data:
	bash scripts/download_sift.sh ./data

# ---- Build ----------------------------------------------------------------

.PHONY: build
build:
	go build -o bin/loader ./cmd/loader
	go build -o bin/bench ./cmd/bench
	go build -o bin/chaos ./cmd/chaos
	go build -o bin/groundtruth ./cmd/groundtruth

# ---- Load -----------------------------------------------------------------

.PHONY: load load-5 load-fresh load-fresh-5
load: build
	./bin/loader \
	  -hosts $(HOSTS_3) \
	  -collection sift1m \
	  -hdf5 ./data/sift-128-euclidean.hdf5 \
	  -shard-number 6 \
	  -replication-factor 2 \
	  -write-consistency 1 \
	  -batch 1024 \
	  -concurrency 8

load-5: build
	./bin/loader \
	  -hosts $(HOSTS_5) \
	  -collection sift1m \
	  -hdf5 ./data/sift-128-euclidean.hdf5 \
	  -shard-number 10 \
	  -replication-factor 2 \
	  -write-consistency 1 \
	  -batch 1024 \
	  -concurrency 8

load-fresh: build cluster-down cluster-up-3 data load
load-fresh-5: build cluster-down cluster-up-5 data load-5

# ---- Benchmark sweeps -----------------------------------------------------

.PHONY: bench bench-5 bench-quick bench-full bench-batch
bench: build
	./bin/bench \
	  -hosts $(HOSTS_3) \
	  -collection sift1m \
	  -workload A,B,C,D,E,F \
	  -concurrency 1,10,15,25,30 \
	  -batch 1 \
	  -duration 30s \
	  -warmup 5s \
	  -k 10 \
	  -recall

bench-5: build
	./bin/bench \
	  -hosts $(HOSTS_5) \
	  -collection sift1m \
	  -workload A,B,C,D,E,F \
	  -concurrency 1,10,15,25,30 \
	  -batch 1 \
	  -duration 30s \
	  -warmup 5s \
	  -k 10 \
	  -recall \
	  -run-label 5node

# Faster smoke run for iteration
bench-quick: build
	./bin/bench \
	  -hosts $(HOSTS_3) \
	  -collection sift1m \
	  -workload B,C \
	  -concurrency 10 \
	  -duration 10s \
	  -warmup 2s

# The "full" sweep including write-consistency variants. Re-creates the
# collection between configs so write_consistency_factor actually changes.
bench-full: build
	@./scripts/run_full_sweep.sh

# Batch-size sensitivity for write-heavy workloads
bench-batch: build
	./bin/bench \
	  -hosts $(HOSTS_3) \
	  -workload A \
	  -concurrency 16 \
	  -batch 1,16,64,256,1024 \
	  -duration 30s \
	  -warmup 5s

# ---- Chaos ----------------------------------------------------------------

.PHONY: chaos chaos-kill chaos-partition chaos-kill-5 chaos-partition-5

chaos: chaos-kill

chaos-kill: build
	./bin/chaos \
	  -hosts $(HOSTS_3) \
	  -rest-urls $(REST_3) \
	  -containers $(CONTAINERS_3) \
	  -mode kill \
	  -workload A \
	  -concurrency 16 \
	  -duration 120s \
	  -inject-after 30s \
	  -recover-after 60s \
	  -kill-target qdrant_node2 \
	  -out-dir results/chaos_kill_node2

chaos-partition: build
	./bin/chaos \
	  -hosts $(HOSTS_3) \
	  -rest-urls $(REST_3) \
	  -containers $(CONTAINERS_3) \
	  -mode partition \
	  -workload A \
	  -concurrency 16 \
	  -duration 120s \
	  -inject-after 30s \
	  -recover-after 60s \
	  -kill-target qdrant_node2 \
	  -out-dir results/chaos_partition_node2

chaos-kill-5: build
	./bin/chaos \
	  -hosts $(HOSTS_5) \
	  -rest-urls $(REST_5) \
	  -containers $(CONTAINERS_5) \
	  -mode kill \
	  -workload A \
	  -concurrency 16 \
	  -duration 120s \
	  -inject-after 30s \
	  -recover-after 60s \
	  -kill-target qdrant_node3 \
	  -out-dir results/chaos_kill_5node

chaos-partition-5: build
	./bin/chaos \
	  -hosts $(HOSTS_5) \
	  -rest-urls $(REST_5) \
	  -containers $(CONTAINERS_5) \
	  -mode partition \
	  -workload A \
	  -concurrency 16 \
	  -duration 120s \
	  -inject-after 30s \
	  -recover-after 60s \
	  -kill-target qdrant_node3 \
	  -out-dir results/chaos_partition_5node

# ---- Compaction sweep -----------------------------------------------------

.PHONY: compaction-sweep
compaction-sweep: build
	@./scripts/run_compaction_sweep.sh

# ---- Reporting ------------------------------------------------------------

.PHONY: report
report:
	python3 scripts/plot_results.py results

# ---- Help -----------------------------------------------------------------

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' Makefile | awk 'BEGIN{FS = ":.*?## "}; {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}'
