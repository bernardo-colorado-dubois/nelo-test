SHELL := /bin/bash
VENV_PY := venv/bin/python

.PHONY: pipeline read transform loop check-creds install clean

pipeline: read transform

read:
	$(VENV_PY) read_queue.py

transform:
	$(VENV_PY) transform_messages.py

loop:
	$(VENV_PY) read_queue.py --loop

check-creds:
	set -a && source .env && set +a && aws sts get-caller-identity

install:
	python3 -m venv venv
	$(VENV_PY) -m pip install --upgrade pip -q
	$(VENV_PY) -m pip install -r requirements.txt -q

clean:
	rm -rf data output/items_flat.csv __pycache__ src/__pycache__
