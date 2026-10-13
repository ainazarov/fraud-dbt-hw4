SHELL := /bin/sh
.DEFAULT_GOAL := help
.NOTPARALLEL:

COMPOSE := docker compose
TOOLS := $(COMPOSE) run --rm -T tools
DATASET ?= demo

.PHONY: help build up down reset demo full pipeline load-demo load-full deps seed run test unit-test data-test docs docs-serve lint lint-fix python-test precommit verify

help:
	@printf '%s\n' 'make demo        Load included labelled sample and run the complete pipeline' 'make full        Load data/train.csv and run the complete pipeline' 'make test        Run dbt data and native unit tests' 'make lint        Lint project SQL with SQLFluff' 'make precommit   Run configured hooks on tracked files' 'make verify      Compare all marts with independent CSV calculations (DATASET=demo|train)' 'make docs-serve  Serve generated dbt Docs at http://localhost:8080' 'make down        Stop this project' 'make reset       Remove only this project containers and volumes'

build:
	$(COMPOSE) build tools

up: build
	$(COMPOSE) up -d --wait clickhouse

down:
	$(COMPOSE) --profile docs down

reset:
	$(COMPOSE) --profile docs down --volumes --remove-orphans

demo: up load-demo
	$(MAKE) pipeline DATASET=demo

full: up load-full
	$(MAKE) pipeline DATASET=train

pipeline: deps seed run test docs lint python-test verify

load-demo:
	$(TOOLS) python /app/scripts/load_hw4.py /data/demo.csv --dataset demo --manifest /results/import_demo.json

load-full:
	$(TOOLS) python /app/scripts/load_hw4.py /data/train.csv --dataset train --manifest /results/import_train.json

deps:
	$(TOOLS) dbt deps

seed:
	$(TOOLS) dbt seed --full-refresh

run:
	$(TOOLS) dbt run

test:
	$(TOOLS) sh -ec 'dbt test; cp dbt/target/run_results.json /results/dbt_test_results.json'

unit-test:
	$(TOOLS) dbt test --select test_type:unit

data-test:
	$(TOOLS) dbt test --select test_type:data

docs:
	$(TOOLS) dbt docs generate

docs-serve:
	$(COMPOSE) --profile docs up -d docs

lint:
	$(TOOLS) sqlfluff lint dbt/models dbt/tests/*.sql
	$(TOOLS) sqlfluff lint --templater raw dbt/tests/fixtures

lint-fix:
	$(TOOLS) sqlfluff fix dbt/models dbt/tests/*.sql
	$(TOOLS) sqlfluff fix --templater raw dbt/tests/fixtures

python-test:
	$(TOOLS) python -m unittest discover -s /app/tests -v
	$(TOOLS) python /app/scripts/audit_hw4.py --self-test

precommit:
	$(TOOLS) pre-commit run --all-files

verify:
	$(TOOLS) python /app/scripts/audit_hw4.py /data/$(DATASET).csv --output /results/audit_$(DATASET).json
