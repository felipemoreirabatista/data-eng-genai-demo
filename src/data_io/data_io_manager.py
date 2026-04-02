import logging
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession

from src.core.exceptions import DataSourceNotFoundError

logger = logging.getLogger("top10_pipeline")


class DataIOManager:
    """Manages data I/O using the Strategy Pattern.

    Resolves logical source IDs to physical paths via a catalog
    configuration and supports multiple data formats (JSON, CSV, Parquet).
    """

    def __init__(
        self,
        spark: SparkSession,
        catalog: Dict[str, Any],
        output_config: Dict[str, Any],
    ) -> None:
        self._spark = spark
        self._catalog = catalog
        self._output_config = output_config

    def read(self, source_id: str) -> DataFrame:
        """Reads a DataFrame by resolving a logical source ID from the catalog."""
        if source_id not in self._catalog:
            raise DataSourceNotFoundError(source_id)

        source = self._catalog[source_id]
        path = source["path"]
        fmt = source["format"]
        options = source.get("options", {})

        logger.info("Reading source '%s' from '%s' (format=%s)", source_id, path, fmt)

        reader = self._spark.read.format(fmt)
        for key, value in options.items():
            reader = reader.option(key, value)

        return reader.load(path)

    def write(self, df: DataFrame, output_id: str) -> None:
        """Writes a DataFrame to the configured output destination."""
        if output_id not in self._output_config:
            raise DataSourceNotFoundError(output_id)

        out = self._output_config[output_id]
        path = out["path"]
        fmt = out.get("format", "parquet")
        mode = out.get("mode", "overwrite")

        logger.info(
            "Writing output '%s' to '%s' (format=%s, mode=%s)",
            output_id,
            path,
            fmt,
            mode,
        )

        df.write.format(fmt).mode(mode).save(path)
