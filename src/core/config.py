"""Módulo responsável pelo carregamento e validação da configuração."""

import os
from typing import Any, Dict

import yaml

from src.core.exceptions import ConfigError


class ConfigLoader:
    """Carrega e valida o arquivo de configuração YAML."""

    def __init__(self, config_path: str = None):
        if config_path is None:
            project_root = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "..")
            )
            config_path = os.path.join(project_root, "config", "config.yaml")

        self._config_path = config_path
        self._config: Dict[str, Any] = {}
        self._project_root = os.path.abspath(
            os.path.join(os.path.dirname(config_path), "..")
        )
        self._load()

    def _load(self) -> None:
        """Carrega o arquivo YAML."""
        if not os.path.exists(self._config_path):
            raise ConfigError(
                f"Arquivo de configuração não encontrado: {self._config_path}"
            )

        try:
            with open(self._config_path, "r", encoding="utf-8") as f:
                self._config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigError(f"Erro ao parsear o arquivo YAML: {e}")

        self._validate()

    def _validate(self) -> None:
        """Valida as seções obrigatórias da configuração."""
        required_sections = ["pipeline", "catalogo"]
        for section in required_sections:
            if section not in self._config:
                raise ConfigError(
                    f"Seção obrigatória '{section}' não encontrada no config."
                )

    @property
    def pipeline(self) -> Dict[str, Any]:
        """Retorna a configuração do pipeline."""
        return self._config["pipeline"]

    @property
    def catalogo(self) -> Dict[str, Any]:
        """Retorna o catálogo de dados."""
        return self._config["catalogo"]

    @property
    def datasets(self) -> Dict[str, Any]:
        """Retorna a configuração dos datasets de exemplo."""
        return self._config.get("datasets", {})

    @property
    def project_root(self) -> str:
        """Retorna o caminho raiz do projeto."""
        return self._project_root

    def resolve_path(self, relative_path: str) -> str:
        """Resolve um caminho relativo em relação à raiz do projeto."""
        return os.path.join(self._project_root, relative_path)
