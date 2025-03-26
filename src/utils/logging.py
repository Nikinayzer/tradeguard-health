import logging
from pathlib import Path

from .paths import get_log_file_path

def setup_logging() -> logging.Logger:
    """Configure logging for the application"""
    # Create logs directory if it doesn't exist
    log_file = get_log_file_path()
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    logger = logging.getLogger('trade_guide_health')
    logger.setLevel(logging.INFO)
    
    return logger 