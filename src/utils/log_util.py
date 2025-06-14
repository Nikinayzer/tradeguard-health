import logging.config
import os

from src.config.config import Config
from src.utils.paths import get_log_file_path

BASE_LOGGER_PREFIX = 'tradeguard.health'
log_file = get_log_file_path()
log_file.parent.mkdir(parents=True, exist_ok=True)

global_level = Config.RS_LOG
kafka_level = global_level if (global_level == "DEBUG") else Config.KAFKA_LOG
job_processor_level = global_level if (global_level == "DEBUG") else Config.RS_LOG_JOB_PROCESSOR

# Create color formatter
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        },
        "colored": {
            "()": "colorlog.ColoredFormatter",
            "format": "%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            "log_colors": {
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "red,bg_white",
            },
            "reset": True,
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "colored",
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
            "level": global_level,
            "propagate": False,
        },
        f"{BASE_LOGGER_PREFIX}.kafka_handler": {
            "handlers": ["console", "file"],
            "level": kafka_level,
            "propagate": False,
        },
        f"{BASE_LOGGER_PREFIX}.job_processor": {
            "handlers": ["console", "file"],
            "level": job_processor_level,
            "propagate": False,
        },
    },
}


def setup_logging() -> None:
    """Set up logging using dictConfig."""
    # Initialize colorama if on Windows for better console color support
    try:
        import colorama
        colorama.init()
    except ImportError:
        # colorama not installed, colors might not work well on Windows
        pass
        
    logging.config.dictConfig(LOGGING_CONFIG)


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
