.PHONY: lint test package data clean

lint:
	black --check src/ tests/
	flake8 src/ tests/

test:
	pytest tests/ -v

package:
	python -m build

data:
	@echo "Baixando dados de exemplo..."
	git clone https://github.com/infobarbosa/dataset-json-clientes ./data/input/dataset-json-clientes || true
	git clone https://github.com/infobarbosa/datasets-csv-pedidos ./data/input/datasets-csv-pedidos || true
	@echo "Dados baixados com sucesso!"

clean:
	rm -rf dist/ build/ *.egg-info src/*.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
