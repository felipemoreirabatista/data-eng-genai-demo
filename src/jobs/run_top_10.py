import logging

from src.data_io.data_io_manager import DataIOManager
from src.transforms.vendas_transforms import VendasTransforms

logger = logging.getLogger("top10_pipeline")


class RunTop10Job:
    """Orchestrates the Top 10 Clients pipeline.

    Reads input data, applies transformations, and writes the result.
    """

    def __init__(
        self,
        data_io: DataIOManager,
        transforms: VendasTransforms,
    ) -> None:
        self._data_io = data_io
        self._transforms = transforms

    def run(self) -> None:
        """Executes the full pipeline."""
        logger.info("Starting Top 10 Clients pipeline...")

        logger.info("Reading pedidos data...")
        df_pedidos = self._data_io.read("pedidos")
        logger.info("Pedidos count: %d", df_pedidos.count())

        logger.info("Reading clientes data...")
        df_clientes = self._data_io.read("clientes")
        logger.info("Clientes count: %d", df_clientes.count())

        logger.info("Calculating Top 10 Clients...")
        df_top_10 = self._transforms.top_10_clientes(df_pedidos, df_clientes)

        logger.info("Top 10 Clients ranking:")
        df_top_10.show(truncate=False)

        logger.info("Writing results...")
        self._data_io.write(df_top_10, "top_10_clientes")

        logger.info("Pipeline completed successfully!")
