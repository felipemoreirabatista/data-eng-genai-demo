"""Exceções customizadas do pipeline de dados."""


class ConfigError(Exception):
    """Erro relacionado ao carregamento ou validação de configuração."""

    pass


class DataIOError(Exception):
    """Erro relacionado à leitura ou escrita de dados."""

    pass


class TransformError(Exception):
    """Erro relacionado à transformação de dados."""

    pass
