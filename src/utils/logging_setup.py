"""Configuração centralizada de logging."""

import logging
import sys


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """Configura e retorna o logger principal da aplicação."""
    logger = logging.getLogger("pipeline")

    if logger.handlers:
        return logger

    logger.setLevel(level)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger
