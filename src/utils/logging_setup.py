import logging
import sys


class LoggingSetup:
    """Configures application-wide logging."""

    @staticmethod
    def configure(level: int = logging.INFO) -> logging.Logger:
        """Configures and returns the root application logger."""
        logger = logging.getLogger("top10_pipeline")
        logger.setLevel(level)

        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(level)
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger
