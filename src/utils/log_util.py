import logging.config
import os
from .paths import get_log_file_path
from ..config.config import Config

BASE_LOGGER_PREFIX = 'tradeguard.health'
log_file = get_log_file_path()
log_file.parent.mkdir(parents=True, exist_ok=True)

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
        },
        "file": {
            "class": "logging.FileHandler",
            "formatter": "standard",
            "filename": str(log_file),
        },
    },
    "loggers": {
        BASE_LOGGER_PREFIX: {
            "handlers": ["console", "file"],
            "level": Config.LOG_LEVEL,
            "propagate": False,
        },
        f"{BASE_LOGGER_PREFIX}.kafka_handler": {
            "handlers": ["console", "file"],
            "level": Config.LOG_LEVEL_KAFKA,
            "propagate": False,
        },
    },
}


def setup_logging() -> logging.Logger:
    """Set up logging using dictConfig."""
    logging.config.dictConfig(LOGGING_CONFIG)
    return logging.getLogger(BASE_LOGGER_PREFIX)


def get_logger(suffix: str = None) -> logging.Logger:
    """
    Retrieve a logger instance with a name appended to the base prefix.

    If a suffix is provided, it is appended to 'tradeguard.health'.
    Otherwise, the caller's file name (without extension) is used.
    """
    if suffix:
        logger_name = f'{BASE_LOGGER_PREFIX}.{suffix}'
    else:
        import inspect
        frame = inspect.stack()[1]
        module_file = frame.frame.f_globals.get('__file__')
        module_name = os.path.splitext(os.path.basename(module_file))[0] if module_file else 'unknown'
        logger_name = f'{BASE_LOGGER_PREFIX}.{module_name}'
    return logging.getLogger(logger_name)
