"""
State Manager

Manages storage instances for different types of data in the application.
"""
from datetime import datetime
from typing import Dict, List, Optional

from src.models import Job, Position, Equity, AtomicPattern, CompositePattern
from src.state.job_storage import JobStorage
from src.state.position_storage import PositionStorage
from src.state.equity_storage import EquityStorage
from src.state.pattern_storage import PatternStorage
from src.utils.log_util import get_logger

logger = get_logger()


class StateManager:
    """Manages storage instances for application state."""

    def __init__(self):
        """Initialize storage instances."""
        self._job_storage = JobStorage()
        self._position_storage = PositionStorage()
        self._equity_storage = EquityStorage()
        self._pattern_storage = PatternStorage()
        logger.info("State manager initialized with storage instances")

    @property
    def job_storage(self) -> JobStorage:
        """Get job storage instance."""
        return self._job_storage

    @property
    def position_storage(self) -> PositionStorage:
        """Get position storage instance."""
        return self._position_storage

    @property
    def equity_storage(self) -> EquityStorage:
        """Get equity storage instance."""
        return self._equity_storage

    @property
    def pattern_storage(self) -> PatternStorage:
        """Get pattern storage instance."""
        return self._pattern_storage

    def clear_old_data(self, hours: int = 24) -> None:
        """Clear old data from all storage instances."""
        self._job_storage.clear_old_jobs(hours)
        self._position_storage.clear_old_positions(hours)
        self._equity_storage.clear_old_equities(hours)
        self._pattern_storage.clear_old_patterns(hours)
        logger.info(f"Cleared data older than {hours} hours from all storage instances")