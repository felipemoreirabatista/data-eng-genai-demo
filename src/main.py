"""Ponto de entrada da aplicação — Composition Root."""

import os
import subprocess
import sys

from src.core.config import ConfigLoader
from src.data_io.data_io_manager import DataIOManager
from src.jobs.run_top_10 import RunTop10Job
from src.utils.logging_setup import setup_logging


def download_datasets(config: ConfigLoader) -> None:
    """Baixa os datasets de exemplo caso não existam localmente."""
    logger = setup_logging()
    datasets = config.datasets

    for name, ds_config in datasets.items():
        local_path = config.resolve_path(ds_config["local_path"])
        repo_url = ds_config["repo_url"]

        if os.path.exists(local_path):
            logger.info(f"Dataset '{name}' já existe em: {local_path}")
            continue

        logger.info(f"Baixando dataset '{name}' de {repo_url}...")
        try:
            subprocess.run(
                ["git", "clone", repo_url, local_path],
                check=True,
                capture_output=True,
                text=True,
            )
            logger.info(f"Dataset '{name}' baixado com sucesso.")
        except subprocess.CalledProcessError as e:
            logger.error(f"Erro ao baixar dataset '{name}': {e.stderr}")
            sys.exit(1)


def main() -> None:
    """Função principal — Composition Root."""
    logger = setup_logging()
    logger.info("Iniciando aplicação...")

    # 1. Carregar configuração
    config = ConfigLoader()

    # 2. Baixar datasets de exemplo
    download_datasets(config)

    # 3. Instanciar DataIOManager (Injeção de Dependência)
    data_io = DataIOManager(
        catalogo=config.catalogo,
        project_root=config.project_root,
    )

    # 4. Instanciar e executar o Job
    job = RunTop10Job(
        data_io=data_io,
        pipeline_config=config.pipeline,
    )
    job.execute()

    logger.info("Aplicação finalizada.")


if __name__ == "__main__":
    main()
