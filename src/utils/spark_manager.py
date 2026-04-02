from typing import Dict, Optional

from pyspark.sql import SparkSession


class SparkManager:
    """Factory for creating and managing SparkSession instances."""

    def __init__(self, spark_config: Dict[str, str]) -> None:
        self._app_name = spark_config.get("app_name", "SparkApp")
        self._master = spark_config.get("master", "local[*]")
        self._session: Optional[SparkSession] = None

    def create_session(self) -> SparkSession:
        """Creates and returns a new SparkSession."""
        self._session = (
            SparkSession.builder.appName(self._app_name)
            .master(self._master)
            .getOrCreate()
        )
        return self._session

    def stop_session(self) -> None:
        """Stops the active SparkSession."""
        if self._session is not None:
            self._session.stop()
            self._session = None
