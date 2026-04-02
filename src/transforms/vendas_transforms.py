from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class VendasTransforms:
    """Pure transformation logic for sales data analysis.

    All methods receive DataFrames and return DataFrames,
    ensuring full testability without side effects.
    """

    @staticmethod
    def calcular_valor_total(df_pedidos: DataFrame) -> DataFrame:
        """Adds a 'valor_total' column (VALOR_UNITARIO * QUANTIDADE)."""
        return df_pedidos.withColumn(
            "valor_total",
            F.col("VALOR_UNITARIO") * F.col("QUANTIDADE"),
        )

    @staticmethod
    def agregar_por_cliente(df: DataFrame) -> DataFrame:
        """Aggregates total purchase value per client."""
        return df.groupBy("ID_CLIENTE").agg(
            F.sum("valor_total").alias("total_compras"),
            F.count("*").alias("qtd_pedidos"),
        )

    @staticmethod
    def top_10_clientes(df_pedidos: DataFrame, df_clientes: DataFrame) -> DataFrame:
        """Full pipeline: calculates totals, aggregates, joins with clients,
        sorts descending, and limits to top 10."""
        df_com_total = VendasTransforms.calcular_valor_total(df_pedidos)
        df_agregado = VendasTransforms.agregar_por_cliente(df_com_total)

        df_ranking = (
            df_agregado.join(
                df_clientes,
                df_agregado["ID_CLIENTE"] == df_clientes["id"],
                "inner",
            )
            .select(
                F.col("ID_CLIENTE"),
                F.col("nome"),
                F.col("email"),
                F.col("total_compras"),
                F.col("qtd_pedidos"),
            )
            .orderBy(F.col("total_compras").desc())
            .limit(10)
        )

        return df_ranking
