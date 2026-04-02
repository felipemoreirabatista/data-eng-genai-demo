.PHONY: lint test package run clean install

install:
	pip install -e ".[dev]"

lint:
	black --check src/ tests/
	flake8 src/ tests/

lint-fix:
	black src/ tests/

test:
	pytest -v

package:
	python -m build

run:
	python -m src.main

clean:
	rm -rf dist/ build/ *.egg-info src/*.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
