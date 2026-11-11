# StreamPulse Engine: common tasks. Windows users without make can copy the commands.
PY ?= python
COMPOSE ?= docker compose
SPARK_IMAGE ?= streampulse/spark:latest

.PHONY: help up down logs ps reset reset-checkpoints test test-spark test-integration bench bench-load report lint psql topics capture

help:            ## list targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*##/\t/'

up:              ## build and start the whole stack
	$(COMPOSE) up -d --build

down:            ## stop the stack (keeps volumes)
	$(COMPOSE) down

reset:           ## stop and delete ALL data (Kafka, Postgres, checkpoints)
	$(COMPOSE) down -v

reset-checkpoints: ## restart Spark from scratch (needed after changing shuffle partitions)
	$(COMPOSE) rm -sf stream-processor spark-worker
	docker volume rm streampulse_spark-checkpoints || true
	$(COMPOSE) up -d spark-worker stream-processor

logs:            ## follow the streaming job and producer logs
	$(COMPOSE) logs -f stream-processor producer

ps:              ## container status
	$(COMPOSE) ps

test:            ## unit tests (pure Python: producer, detectors, alerts)
	$(PY) -m pytest -m "not spark and not integration"

test-spark:      ## Spark transformation tests, run inside the Spark image (no local Java needed)
	docker run --rm --user root -v "$(CURDIR)":/w -w /w $(SPARK_IMAGE) bash -c \
	  'pip3 install -q pytest && PYTHONPATH=/w:/opt/spark/python:$$(ls /opt/spark/python/lib/py4j-*.zip) python3 -m pytest -q -m spark -p no:cacheprovider'

test-integration: ## end-to-end tests against the running stack
	STREAMPULSE_INTEGRATION=1 $(PY) -m pytest -m integration -v

bench:           ## offline detector evaluation (reproducible, ~2 min)
	$(PY) -m benchmarks.detection_evaluation --hours 6 --devices 20 --seeds 1 2 3
	$(PY) -m benchmarks.generate_reports

bench-load:      ## live load test against the running stack (~15 min)
	$(PY) -m benchmarks.load_test --rates 100 500 1000 2000 4000 --devices 50
	$(PY) -m benchmarks.generate_reports

report:          ## re-render benchmark charts and REPORT.md
	$(PY) -m benchmarks.generate_reports

lint:            ## ruff
	$(PY) -m ruff check .

psql:            ## open a psql shell
	$(COMPOSE) exec postgres psql -U streampulse -d streampulse

topics:          ## describe Kafka topics
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe

capture:         ## dashboard screenshots into docs/images (needs playwright + Edge/Chrome)
	$(PY) scripts/capture_dashboard.py --out docs/images
