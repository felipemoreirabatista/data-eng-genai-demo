"""Lógica pura de transformação de dados de vendas."""

import pandas as pd

from src.core.exceptions import TransformError


class VendasTransforms:
    """Transformações puras sobre DataFrames de vendas."""

    @staticmethod
    def calcular_valor_total(pedidos_df: pd.DataFrame) -> pd.DataFrame:
        """Calcula o valor total de cada pedido."""
        required_cols = {"VALOR_UNITARIO", "QUANTIDADE"}
        missing = required_cols - set(pedidos_df.columns)
        if missing:
            raise TransformError(f"Colunas obrigatórias ausentes: {missing}")

        df = pedidos_df.copy()
        df["VALOR_TOTAL"] = df["VALOR_UNITARIO"] * df["QUANTIDADE"]
        return df

    @staticmethod
    def agregar_por_cliente(pedidos_df: pd.DataFrame) -> pd.DataFrame:
        """Agrega o valor total por cliente."""
        required_cols = {"ID_CLIENTE", "VALOR_TOTAL"}
        missing = required_cols - set(pedidos_df.columns)
        if missing:
            raise TransformError(f"Colunas obrigatórias ausentes: {missing}")

        return pedidos_df.groupby("ID_CLIENTE", as_index=False).agg(
            VALOR_TOTAL_COMPRAS=("VALOR_TOTAL", "sum")
        )

    @staticmethod
    def rankear_top_n(agregado_df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
        """Retorna os Top N clientes por valor total de compras."""
        return (
            agregado_df.sort_values("VALOR_TOTAL_COMPRAS", ascending=False)
            .head(n)
            .reset_index(drop=True)
        )

    @staticmethod
    def enriquecer_com_clientes(
        ranking_df: pd.DataFrame, clientes_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Enriquece o ranking com dados dos clientes (nome, email)."""
        colunas_cliente = ["id", "nome", "email"]
        available = [c for c in colunas_cliente if c in clientes_df.columns]

        if "id" not in available:
            raise TransformError(
                "Coluna 'id' não encontrada no DataFrame de clientes."
            )

        clientes_subset = clientes_df[available].copy()

        return ranking_df.merge(
            clientes_subset,
            left_on="ID_CLIENTE",
            right_on="id",
            how="left",
        ).drop(columns=["id"])
