.PHONY: mkvenv install test clean

mkvenv:
	python -m venv venv && source ./venv/bin/activate && pip install -U pip

install:
	source ./venv/bin/activate && pip install -e .

test:
	source ./venv/bin/activate && python -m unittest

clean:
	rm -rf venv
