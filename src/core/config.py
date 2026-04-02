from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from src.core.exceptions import ConfigNotFoundError


class ConfigLoader:
    """Loads and parses the application configuration from config.yaml."""

    def __init__(self, config_path: Optional[str] = None) -> None:
        if config_path is None:
            project_root = Path(__file__).resolve().parent.parent.parent
            config_path = str(project_root / "config" / "config.yaml")

        self._config_path = Path(config_path)

        if not self._config_path.exists():
            raise ConfigNotFoundError(str(self._config_path))

        with open(self._config_path, "r", encoding="utf-8") as f:
            self._config: Dict[str, Any] = yaml.safe_load(f)

    def get_spark_config(self) -> Dict[str, str]:
        """Returns Spark configuration parameters."""
        return self._config.get("spark", {})

    def get_catalog(self) -> Dict[str, Any]:
        """Returns the data catalog with logical source definitions."""
        return self._config.get("catalog", {})

    def get_output_config(self) -> Dict[str, Any]:
        """Returns the output configuration."""
        return self._config.get("output", {})
