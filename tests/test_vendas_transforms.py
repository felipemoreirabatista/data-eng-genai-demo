"""Testes unitários para VendasTransforms."""

import pandas as pd
import pytest

from src.core.exceptions import TransformError
from src.transforms.vendas_transforms import VendasTransforms


@pytest.fixture
def transforms():
    """Fixture que retorna uma instância de VendasTransforms."""
    return VendasTransforms()


@pytest.fixture
def pedidos_df():
    """Fixture com dados sintéticos de pedidos."""
    return pd.DataFrame(
        {
            "ID_PEDIDO": ["p1", "p2", "p3", "p4", "p5"],
            "PRODUTO": [
                "NOTEBOOK",
                "CELULAR",
                "GELADEIRA",
                "NOTEBOOK",
                "CELULAR",
            ],
            "VALOR_UNITARIO": [1500.0, 1000.0, 2000.0, 1500.0, 1000.0],
            "QUANTIDADE": [2, 3, 1, 1, 5],
            "DATA_CRIACAO": [
                "2026-01-01",
                "2026-01-02",
                "2026-01-03",
                "2026-01-04",
                "2026-01-05",
            ],
            "UF": ["SP", "RJ", "MG", "SP", "RJ"],
            "ID_CLIENTE": [1, 2, 3, 1, 2],
        }
    )


@pytest.fixture
def clientes_df():
    """Fixture com dados sintéticos de clientes."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "nome": [
                "Alice Silva",
                "Bob Santos",
                "Carol Oliveira",
                "David Costa",
                "Eva Lima",
            ],
            "email": [
                "alice@email.com",
                "bob@email.com",
                "carol@email.com",
                "david@email.com",
                "eva@email.com",
            ],
        }
    )


class TestCalcularValorTotal:
    """Testes para o método calcular_valor_total."""

    def test_calcula_valor_total_corretamente(self, transforms, pedidos_df):
        resultado = transforms.calcular_valor_total(pedidos_df)
        assert "VALOR_TOTAL" in resultado.columns
        assert resultado["VALOR_TOTAL"].tolist() == [
            3000.0,
            3000.0,
            2000.0,
            1500.0,
            5000.0,
        ]

    def test_nao_modifica_dataframe_original(self, transforms, pedidos_df):
        transforms.calcular_valor_total(pedidos_df)
        assert "VALOR_TOTAL" not in pedidos_df.columns

    def test_erro_colunas_ausentes(self, transforms):
        df = pd.DataFrame({"PRODUTO": ["A"], "QUANTIDADE": [1]})
        with pytest.raises(TransformError, match="Colunas obrigatórias"):
            transforms.calcular_valor_total(df)


class TestAgregarPorCliente:
    """Testes para o método agregar_por_cliente."""

    def test_agrega_corretamente(self, transforms, pedidos_df):
        pedidos_com_total = transforms.calcular_valor_total(pedidos_df)
        resultado = transforms.agregar_por_cliente(pedidos_com_total)

        assert len(resultado) == 3
        assert "ID_CLIENTE" in resultado.columns
        assert "VALOR_TOTAL_COMPRAS" in resultado.columns

        cliente_1 = resultado[resultado["ID_CLIENTE"] == 1]
        assert cliente_1["VALOR_TOTAL_COMPRAS"].values[0] == 4500.0

        cliente_2 = resultado[resultado["ID_CLIENTE"] == 2]
        assert cliente_2["VALOR_TOTAL_COMPRAS"].values[0] == 8000.0

        cliente_3 = resultado[resultado["ID_CLIENTE"] == 3]
        assert cliente_3["VALOR_TOTAL_COMPRAS"].values[0] == 2000.0

    def test_erro_colunas_ausentes(self, transforms):
        df = pd.DataFrame({"ID_CLIENTE": [1], "VALOR": [100]})
        with pytest.raises(TransformError, match="Colunas obrigatórias"):
            transforms.agregar_por_cliente(df)


class TestRankearTopN:
    """Testes para o método rankear_top_n."""

    def test_retorna_top_n(self, transforms):
        df = pd.DataFrame(
            {
                "ID_CLIENTE": [1, 2, 3, 4, 5],
                "VALOR_TOTAL_COMPRAS": [100, 500, 300, 200, 400],
            }
        )
        resultado = transforms.rankear_top_n(df, n=3)

        assert len(resultado) == 3
        assert resultado["ID_CLIENTE"].tolist() == [2, 5, 3]
        assert resultado["VALOR_TOTAL_COMPRAS"].tolist() == [500, 400, 300]

    def test_retorna_todos_se_n_maior(self, transforms):
        df = pd.DataFrame(
            {
                "ID_CLIENTE": [1, 2],
                "VALOR_TOTAL_COMPRAS": [100, 200],
            }
        )
        resultado = transforms.rankear_top_n(df, n=10)
        assert len(resultado) == 2

    def test_top_n_default_10(self, transforms):
        df = pd.DataFrame(
            {
                "ID_CLIENTE": list(range(1, 21)),
                "VALOR_TOTAL_COMPRAS": list(range(100, 2100, 100)),
            }
        )
        resultado = transforms.rankear_top_n(df)
        assert len(resultado) == 10


class TestEnriquecerComClientes:
    """Testes para o método enriquecer_com_clientes."""

    def test_enriquece_corretamente(self, transforms, clientes_df):
        ranking_df = pd.DataFrame(
            {
                "ID_CLIENTE": [2, 1, 3],
                "VALOR_TOTAL_COMPRAS": [8000.0, 4500.0, 2000.0],
            }
        )
        resultado = transforms.enriquecer_com_clientes(ranking_df, clientes_df)

        assert "nome" in resultado.columns
        assert "email" in resultado.columns
        assert "id" not in resultado.columns
        assert resultado["nome"].tolist() == [
            "Bob Santos",
            "Alice Silva",
            "Carol Oliveira",
        ]

    def test_cliente_nao_encontrado_retorna_nan(self, transforms, clientes_df):
        ranking_df = pd.DataFrame(
            {
                "ID_CLIENTE": [999],
                "VALOR_TOTAL_COMPRAS": [1000.0],
            }
        )
        resultado = transforms.enriquecer_com_clientes(ranking_df, clientes_df)
        assert pd.isna(resultado["nome"].values[0])

    def test_erro_sem_coluna_id(self, transforms):
        ranking_df = pd.DataFrame({"ID_CLIENTE": [1]})
        clientes_df = pd.DataFrame({"nome": ["Alice"]})
        with pytest.raises(TransformError, match="Coluna 'id'"):
            transforms.enriquecer_com_clientes(ranking_df, clientes_df)
