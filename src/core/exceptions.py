class ConfigNotFoundError(Exception):
    """Raised when the config.yaml file is not found."""

    def __init__(self, path: str) -> None:
        super().__init__(f"Configuration file not found: {path}")
        self.path = path


class DataSourceNotFoundError(Exception):
    """Raised when a logical data source ID is not found in the catalog."""

    def __init__(self, source_id: str) -> None:
        super().__init__(
            f"Data source '{source_id}' not found in catalog configuration."
        )
        self.source_id = source_id


class TransformError(Exception):
    """Raised when an error occurs during a data transformation."""

    def __init__(self, message: str) -> None:
        super().__init__(f"Transform error: {message}")
