PYTHON ?= python3.12
VENV_PYTHON := .venv/bin/python

.DEFAULT_GOAL := help
.PHONY: help install ingest load transform run test clean check-env

help:
	@printf '%s\n' \
	  'make install    Create .venv and install dependencies (Python 3.12)' \
	  'make ingest     Download and validate the latest raw CSV' \
	  'make load       Replace the raw DuckDB table from the local CSV' \
	  'make transform  Clean, validate, and model the loaded raw data' \
	  'make run        Run ingest, load, and transform sequentially' \
	  'make test       Run tests' \
	  'make clean      Remove caches only; keep all data and .venv' \
	  'Override Python with: make install PYTHON=/path/to/python3.12'

install:
	$(PYTHON) -c "import sys; sys.exit(0 if sys.version_info[:2] == (3, 12) else 'Python 3.12 is required. Set PYTHON to a Python 3.12 interpreter.')"
	$(PYTHON) -m venv .venv
	$(VENV_PYTHON) -m pip install -r requirements.txt

check-env:
	@test -x $(VENV_PYTHON) || { printf '%s\n' 'Run make install first.' >&2; exit 1; }

ingest: check-env
	$(VENV_PYTHON) -m src.ingest

load: check-env
	$(VENV_PYTHON) -m src.load

transform: check-env
	$(VENV_PYTHON) -m src.pipeline

run: check-env
	$(MAKE) ingest
	$(MAKE) load
	$(MAKE) transform

test: check-env
	$(VENV_PYTHON) -m pytest tests/ -q

clean:
	rm -rf .pytest_cache __pycache__
	find src tests notebooks -type d \( -name __pycache__ -o -name .ipynb_checkpoints \) -prune -exec rm -rf {} +
