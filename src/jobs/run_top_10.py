"""Job de orquestração do pipeline Top 10 Clientes."""

import logging
from typing import Any, Dict

from src.data_io.data_io_manager import DataIOManager
from src.transforms.vendas_transforms import VendasTransforms

logger = logging.getLogger("pipeline")


class RunTop10Job:
    """Orquestra o pipeline para identificar os Top 10 Clientes."""

    def __init__(
        self,
        data_io: DataIOManager,
        pipeline_config: Dict[str, Any],
    ):
        self._data_io = data_io
        self._top_n = pipeline_config.get("top_n", 10)
        self._transforms = VendasTransforms()

    def execute(self) -> None:
        """Executa o pipeline completo."""
        logger.info("=== Iniciando pipeline Top 10 Clientes ===")

        # 1. Leitura dos dados
        logger.info("Etapa 1: Leitura dos dados de pedidos...")
        pedidos_df = self._data_io.read("pedidos_bronze")
        logger.info(f"  Pedidos carregados: {len(pedidos_df)} registros")

        logger.info("Etapa 2: Leitura dos dados de clientes...")
        clientes_df = self._data_io.read("clientes_bronze")
        logger.info(f"  Clientes carregados: {len(clientes_df)} registros")

        # 2. Transformações
        logger.info("Etapa 3: Calculando valor total por pedido...")
        pedidos_df = self._transforms.calcular_valor_total(pedidos_df)

        logger.info("Etapa 4: Agregando valor total por cliente...")
        agregado_df = self._transforms.agregar_por_cliente(pedidos_df)

        logger.info(f"Etapa 5: Rankeando Top {self._top_n} clientes...")
        ranking_df = self._transforms.rankear_top_n(agregado_df, self._top_n)

        logger.info("Etapa 6: Enriquecendo com dados dos clientes...")
        resultado_df = self._transforms.enriquecer_com_clientes(
            ranking_df, clientes_df
        )

        # 3. Escrita do resultado
        logger.info("Etapa 7: Salvando resultado...")
        output_path = self._data_io.write(resultado_df, "top_10_clientes")

        logger.info("=== Pipeline concluído com sucesso! ===")
        logger.info(f"Resultado salvo em: {output_path}")
        top_str = resultado_df.to_string(index=False)
        logger.info(f"\nTop {self._top_n} Clientes:\n{top_str}")
