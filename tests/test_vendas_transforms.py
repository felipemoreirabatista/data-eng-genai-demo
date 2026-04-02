import pytest
from pyspark.sql import SparkSession

from src.transforms.vendas_transforms import VendasTransforms


@pytest.fixture(scope="session")
def spark():
    """Creates a SparkSession for testing."""
    session = (
        SparkSession.builder.appName("TestTop10Pipeline")
        .master("local[1]")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestVendasTransforms:
    """Tests for VendasTransforms transformation logic."""

    def test_calcular_valor_total(self, spark):
        """Tests that valor_total is computed as VALOR_UNITARIO * QUANTIDADE."""
        data = [
            ("pedido1", "NOTEBOOK", 1500.0, 2, "2026-01-01", "SP", 1),
            ("pedido2", "CELULAR", 1000.0, 3, "2026-01-02", "RJ", 2),
            ("pedido3", "GELADEIRA", 2000.0, 1, "2026-01-03", "MG", 1),
        ]
        columns = [
            "ID_PEDIDO",
            "PRODUTO",
            "VALOR_UNITARIO",
            "QUANTIDADE",
            "DATA_CRIACAO",
            "UF",
            "ID_CLIENTE",
        ]
        df = spark.createDataFrame(data, columns)

        result = VendasTransforms.calcular_valor_total(df)
        rows = result.collect()

        assert rows[0]["valor_total"] == 3000.0  # 1500 * 2
        assert rows[1]["valor_total"] == 3000.0  # 1000 * 3
        assert rows[2]["valor_total"] == 2000.0  # 2000 * 1

    def test_agregar_por_cliente(self, spark):
        """Tests aggregation by client: sum of valor_total and order count."""
        data = [
            ("pedido1", "NOTEBOOK", 1500.0, 2, "2026-01-01", "SP", 1, 3000.0),
            ("pedido2", "CELULAR", 1000.0, 3, "2026-01-02", "RJ", 2, 3000.0),
            ("pedido3", "GELADEIRA", 2000.0, 1, "2026-01-03", "MG", 1, 2000.0),
        ]
        columns = [
            "ID_PEDIDO",
            "PRODUTO",
            "VALOR_UNITARIO",
            "QUANTIDADE",
            "DATA_CRIACAO",
            "UF",
            "ID_CLIENTE",
            "valor_total",
        ]
        df = spark.createDataFrame(data, columns)

        result = VendasTransforms.agregar_por_cliente(df)
        rows = {row["ID_CLIENTE"]: row for row in result.collect()}

        assert rows[1]["total_compras"] == 5000.0  # 3000 + 2000
        assert rows[1]["qtd_pedidos"] == 2
        assert rows[2]["total_compras"] == 3000.0
        assert rows[2]["qtd_pedidos"] == 1

    def test_top_10_clientes(self, spark):
        """Tests the full Top 10 pipeline with synthetic data."""
        pedidos_data = [
            (f"pedido{i}", "PRODUTO", 1000.0 * i, i, "2026-01-01", "SP", i)
            for i in range(1, 16)
        ]
        pedidos_columns = [
            "ID_PEDIDO",
            "PRODUTO",
            "VALOR_UNITARIO",
            "QUANTIDADE",
            "DATA_CRIACAO",
            "UF",
            "ID_CLIENTE",
        ]
        df_pedidos = spark.createDataFrame(pedidos_data, pedidos_columns)

        clientes_data = [
            (i, f"Cliente {i}", f"cliente{i}@email.com") for i in range(1, 16)
        ]
        clientes_columns = ["id", "nome", "email"]
        df_clientes = spark.createDataFrame(clientes_data, clientes_columns)

        result = VendasTransforms.top_10_clientes(df_pedidos, df_clientes)
        rows = result.collect()

        # Must return exactly 10 rows
        assert len(rows) == 10

        # Must be ordered descending by total_compras
        totals = [row["total_compras"] for row in rows]
        assert totals == sorted(totals, reverse=True)

        # Top client should be ID 15 (1000*15 * 15 = 225000)
        assert rows[0]["ID_CLIENTE"] == 15
        assert rows[0]["nome"] == "Cliente 15"
        assert rows[0]["total_compras"] == 225000.0

    def test_top_10_with_fewer_clients(self, spark):
        """Tests that the pipeline works with fewer than 10 clients."""
        pedidos_data = [
            ("pedido1", "NOTEBOOK", 1500.0, 2, "2026-01-01", "SP", 1),
            ("pedido2", "CELULAR", 1000.0, 3, "2026-01-02", "RJ", 2),
            ("pedido3", "GELADEIRA", 2000.0, 1, "2026-01-03", "MG", 3),
        ]
        pedidos_columns = [
            "ID_PEDIDO",
            "PRODUTO",
            "VALOR_UNITARIO",
            "QUANTIDADE",
            "DATA_CRIACAO",
            "UF",
            "ID_CLIENTE",
        ]
        df_pedidos = spark.createDataFrame(pedidos_data, pedidos_columns)

        clientes_data = [
            (1, "Alice", "alice@email.com"),
            (2, "Bob", "bob@email.com"),
            (3, "Carol", "carol@email.com"),
        ]
        clientes_columns = ["id", "nome", "email"]
        df_clientes = spark.createDataFrame(clientes_data, clientes_columns)

        result = VendasTransforms.top_10_clientes(df_pedidos, df_clientes)
        rows = result.collect()

        assert len(rows) == 3

        totals = [row["total_compras"] for row in rows]
        assert totals == sorted(totals, reverse=True)
