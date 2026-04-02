"""Módulo de I/O de dados com Strategy Pattern."""

import glob
import logging
import os
from typing import Any, Dict

import pandas as pd

from src.core.exceptions import DataIOError

logger = logging.getLogger("pipeline")


class DataIOManager:
    """Gerencia leitura e escrita de dados.

    Baseado no catálogo de configuração.
    """

    def __init__(self, catalogo: Dict[str, Any], project_root: str):
        self._catalogo = catalogo
        self._project_root = project_root

    def _resolve_path(self, relative_path: str) -> str:
        """Resolve caminho relativo à raiz do projeto."""
        return os.path.join(self._project_root, relative_path)

    def _get_dataset_config(self, dataset_id: str) -> Dict[str, Any]:
        """Obtém a configuração de um dataset pelo ID lógico."""
        if dataset_id not in self._catalogo:
            raise DataIOError(
                f"Dataset '{dataset_id}' não encontrado no catálogo."
            )
        return self._catalogo[dataset_id]

    def read(self, dataset_id: str) -> pd.DataFrame:
        """Lê um dataset pelo seu ID lógico e retorna um DataFrame."""
        config = self._get_dataset_config(dataset_id)
        fmt = config["format"]
        path = self._resolve_path(config["path"])
        options = config.get("options", {})

        logger.info(f"Lendo dataset '{dataset_id}' de: {path}")

        if fmt == "json":
            return self._read_json(path, options)
        elif fmt == "csv":
            return self._read_csv(path, options)
        else:
            raise DataIOError(f"Formato '{fmt}' não suportado para leitura.")

    def write(self, df: pd.DataFrame, dataset_id: str) -> str:
        """Escreve um DataFrame no caminho configurado para o dataset."""
        config = self._get_dataset_config(dataset_id)
        fmt = config["format"]
        path = self._resolve_path(config["path"])
        options = config.get("options", {})

        os.makedirs(path, exist_ok=True)
        output_file = os.path.join(path, f"{dataset_id}.csv")

        logger.info(f"Escrevendo dataset '{dataset_id}' em: {output_file}")

        if fmt == "csv":
            separator = options.get("separator", ",")
            df.to_csv(output_file, sep=separator, index=False)
        else:
            raise DataIOError(f"Formato '{fmt}' não suportado para escrita.")

        return output_file

    def _read_json(self, path: str, options: Dict[str, Any]) -> pd.DataFrame:
        """Lê um arquivo JSON / JSON Lines."""
        if not os.path.exists(path):
            raise DataIOError(f"Arquivo não encontrado: {path}")

        lines = options.get("lines", False)
        try:
            return pd.read_json(path, lines=lines)
        except Exception as e:
            raise DataIOError(f"Erro ao ler JSON '{path}': {e}")

    def _read_csv(self, path: str, options: Dict[str, Any]) -> pd.DataFrame:
        """Lê arquivos CSV de um diretório usando glob pattern."""
        separator = options.get("separator", ",")
        glob_pattern = options.get("glob_pattern", "*.csv")

        if os.path.isdir(path):
            pattern = os.path.join(path, glob_pattern)
            files = sorted(glob.glob(pattern))
            if not files:
                raise DataIOError(
                    f"Nenhum arquivo encontrado com o padrão: {pattern}"
                )
            logger.info(f"Encontrados {len(files)} arquivo(s) CSV.")
            dfs = []
            for file in files:
                df = pd.read_csv(file, sep=separator)
                dfs.append(df)
            return pd.concat(dfs, ignore_index=True)
        elif os.path.isfile(path):
            try:
                return pd.read_csv(path, sep=separator)
            except Exception as e:
                raise DataIOError(f"Erro ao ler CSV '{path}': {e}")
        else:
            raise DataIOError(f"Caminho não encontrado: {path}")
