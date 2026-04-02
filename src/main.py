"""Composition Root - Entry point for the Top 10 Clients pipeline.

Instantiates and injects all dependencies following Clean Architecture.
"""

from src.core.config import ConfigLoader
from src.data_io.data_io_manager import DataIOManager
from src.jobs.run_top_10 import RunTop10Job
from src.transforms.vendas_transforms import VendasTransforms
from src.utils.logging_setup import LoggingSetup
from src.utils.spark_manager import SparkManager


def main() -> None:
    """Application entry point."""
    logger = LoggingSetup.configure()
    logger.info("Initializing Top 10 Clients Pipeline...")

    config = ConfigLoader()
    logger.info("Configuration loaded successfully.")

    spark_manager = SparkManager(config.get_spark_config())
    spark = spark_manager.create_session()
    logger.info("SparkSession created.")

    try:
        data_io = DataIOManager(
            spark=spark,
            catalog=config.get_catalog(),
            output_config=config.get_output_config(),
        )

        transforms = VendasTransforms()

        job = RunTop10Job(data_io=data_io, transforms=transforms)
        job.run()

    finally:
        spark_manager.stop_session()
        logger.info("SparkSession stopped. Pipeline finished.")


if __name__ == "__main__":
    main()
