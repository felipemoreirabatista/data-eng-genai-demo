# Testing: PySpark Top 10 Clientes Pipeline

## Overview
This is a CLI-based PySpark pipeline that identifies the Top 10 Customers by total purchase volume. No external services or UI — all testing is done via terminal commands.

## Prerequisites
- Python 3.9+
- Java (for Spark)
- Dependencies: `pip install pyspark pyyaml pytest black flake8 build`

## Setup
1. Install dependencies: `pip install pyspark pyyaml pytest black flake8 build`
2. Download input data: `make data` (clones datasets from GitHub)
3. This downloads ~6MB of gzipped CSV/JSON data into `data/input/`

## Test Commands

### Lint
```bash
make lint
```
Expected: `black --check` reports 0 reformatted, `flake8` produces zero output.

### Unit Tests
```bash
make test
```
Expected: 4 tests pass (`test_calcular_valor_total`, `test_agregar_por_cliente`, `test_top_10_clientes`, `test_top_10_with_fewer_clients`).

### Full Pipeline (End-to-End)
```bash
python -m src.main
```
Must be run from the **project root** (relative paths in `config/config.yaml`).

Expected output:
- Logs showing pedidos count (~60k) and clientes count (~15k)
- Top 10 Clients ranking table printed to stdout
- Parquet output saved to `data/output/top_10_clientes`

### Output Validation
Read back the Parquet output and verify:
- Exactly 10 rows
- Columns: `ID_CLIENTE`, `nome`, `email`, `total_compras`, `qtd_pedidos`
- `total_compras` in strictly descending order
- No null values in `nome` or `email` (confirms join worked)

### Package Build
```bash
make package
```
Expected: `.whl` file created in `dist/`.

## Architecture Notes
- **Config-Driven**: All paths are in `config/config.yaml`, never hardcoded in `src/`
- **Data files are gzipped**: The upstream datasets provide `.json.gz` and `.csv.gz` files. Spark reads these natively.
- **Clean Architecture**: `main.py` is the Composition Root; transforms are pure functions (DataFrame in → DataFrame out)

## Common Issues
- If pipeline fails with `PATH_NOT_FOUND`, verify you ran `make data` first and are executing from the project root
- The input datasets might change file structure upstream — check actual file extensions if loading fails (e.g., `.json` vs `.json.gz`)
- Join between pedidos and clientes relies on `inferSchema` producing compatible types for `ID_CLIENTE` and `id`. If the join returns 0 rows, add explicit casts.
