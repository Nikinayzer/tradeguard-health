"""
State Management Service

Manages shared application state including job histories and user mappings.
Provides thread-safe access to state and handles state updates from Kafka events.
"""

import logging
import re
from typing import Dict, Any, Optional, List
from threading import Lock
from datetime import datetime, timedelta, timezone

from src.models.job_models import Job
from src.models.position_models import Position
from src.models.equity_models import Equity
from src.utils.log_util import get_logger
from src.state.position_storage import PositionStorage
from src.state.job_storage import JobStorage
from src.state.equity_storage import EquityStorage
from src.config.config import Config

logger = get_logger()


class StateManager:
    """
    Manages shared application state with thread-safe access.
    Provides simple storage and retrieval of jobs.
    """

    def __init__(self):
        """
        Initialize the state manager with empty state.
        """

        self._position_storage = self._initialize_position_storage()
        self._job_storage = self._initialize_job_storage()
        self._equity_storage = self._initialize_equity_storage()

        self._lock = Lock()
        logger.info(f"State manager initialized")

    def _initialize_position_storage(self) -> PositionStorage:
        """
        Initialize position storage based on configuration.

        Returns:
            PositionStorage instance
        """
        return PositionStorage()

    def _initialize_job_storage(self) -> JobStorage:
        """
        Initialize job storage based on configuration.

        Returns:
            JobStorage instance
        """

        return JobStorage()

    def _initialize_equity_storage(self) -> EquityStorage:
        """
        Initialize equity storage based on configuration.

        Returns:
            EquityStorage instance
        """
        return EquityStorage()

    # Job-related methods - delegate to JobStorage
    def store_job(self, job: Job) -> None:
        """
        Store a job in the state.
        
        Args:
            job: The job to store
        """
        try:
            self._job_storage.store_job(job)
        except Exception as e:
            logger.error(f"Error storing job: {e}")

    def get_job(self, job_id: int) -> Optional[Job]:
        """
        Get a job by its ID.
        
        Args:
            job_id: The ID of the job
            
        Returns:
            The job if found, None otherwise
        """
        return self._job_storage.get_job(job_id)

    def get_user_jobs(self, user_id: int, hours: int = 0) -> Dict[int, Job]:
        """
        Get all jobs for a specific user within a specified timeframe.
        
        Args:
            user_id: The ID of the user
            hours: Number of hours to look back (default: 0, meaning all jobs)
            
        Returns:
            Dictionary of jobs for the user within the specified timeframe
        """
        return self._job_storage.get_user_jobs(user_id, hours)

    def get_job_user(self, job_id: int) -> Optional[int]:
        """
        Get the user ID for a specific job.
        
        Args:
            job_id: The ID of the job
            
        Returns:
            User ID if found, None otherwise
        """
        return self._job_storage.get_job_user(job_id)

    def get_jobs_state(self, hours: int = 0) -> Dict[int, Dict[int, Job]]:
        """
        Get a copy of the entire jobs state, optionally filtered by timeframe.
        
        Args:
            hours: Number of hours to look back (default: 0, meaning all jobs)
            
        Returns:
            Dictionary mapping user IDs to their job histories
        """
        return self._job_storage.get_jobs_state(hours)

    def get_dca_jobs(self, hours: int = 0) -> Dict[int, Dict[int, Job]]:
        """
        Get a copy of the DCA jobs state, optionally filtered by timeframe.
        
        Args:
            hours: Number of hours to look back (default: 0, meaning all jobs)
            
        Returns:
            Dictionary mapping user IDs to their DCA jobs
        """
        return self._job_storage.get_dca_jobs(hours)

    def get_liq_jobs(self, hours: int = 0) -> Dict[int, Dict[int, Job]]:
        """
        Get a copy of the LIQ jobs state, optionally filtered by timeframe.
        
        Args:
            hours: Number of hours to look back (default: 0, meaning all jobs)
            
        Returns:
            Dictionary mapping user IDs to their LIQ jobs
        """
        return self._job_storage.get_liq_jobs(hours)

    def get_job_to_user_map(self) -> Dict[int, int]:
        """
        Get a copy of the job_id to user_id mapping.
        
        Returns:
            Dictionary mapping job IDs to user IDs
        """
        return self._job_storage.get_job_to_user_map()

    # Position-related methods - delegate to PositionStorage
    def store_position(self, position: Position) -> bool:
        """
        Store a position in the state.
        
        Args:
            position: The position to store
            
        Returns:
            bool: Whether the position was stored in history
        """
        return self._position_storage.store_position(position)

    def get_position(self, user_id: int, venue: str, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get a position by user, venue, and symbol.
        
        Args:
            user_id: The ID of the user
            venue: The venue of the position
            symbol: The symbol of the position
            
        Returns:
            Position data if found, None otherwise
        """
        return self._position_storage.get_position(user_id, venue, symbol)

    def get_user_positions(self, user_id: int) -> Dict[str, Any]:
        """
        Get all positions for a user.
        
        Args:
            user_id: The ID of the user
            
        Returns:
            Dictionary mapping position keys to positions
        """
        return self._position_storage.get_user_positions(user_id)

    def get_venue_positions(self, venue: str) -> Dict[str, Any]:
        """
        Get all positions for a venue.
        
        Args:
            venue: The venue to get positions for
            
        Returns:
            Dictionary mapping position keys to positions
        """
        return self._position_storage.get_venue_positions(venue)

    def get_positions_state(self) -> Dict[int, Dict[str, Any]]:
        """
        Get a copy of the entire positions state.
        
        Returns:
            Dictionary mapping user IDs to their position dictionaries
        """
        return self._position_storage.get_all_positions()

    def get_position_history(self, user_id: int, venue: str, symbol: str, limit: int = 100) -> List[Dict]:
        """
        Get position history.
        
        Args:
            user_id: The ID of the user
            venue: The venue of the position
            symbol: The symbol of the position
            limit: Maximum number of history items to return
            
        Returns:
            List of position history items
        """
        return self._position_storage.get_position_history(user_id, venue, symbol, limit)

    def get_position_history_by_event_type(self, user_id: int, venue: str, symbol: str,
                                           event_types: List[str], limit: int = 100) -> List[Dict]:
        """
        Get position history filtered by event type.
        
        Args:
            user_id: The ID of the user
            venue: The venue of the position
            symbol: The symbol of the position
            event_types: List of event types to include
            limit: Maximum number of history items to return
            
        Returns:
            List of position history items matching the event types
        """
        return self._position_storage.get_position_history_by_event_type(user_id, venue, symbol, event_types, limit)

    def get_position_timeseries(self, user_id: int, venue: str, symbol: str,
                                start_time: Optional[datetime] = None,
                                end_time: Optional[datetime] = None) -> List[Dict]:
        """
        Get position time series data.
        
        Args:
            user_id: The ID of the user
            venue: The venue of the position
            symbol: The symbol of the position
            start_time: Optional start time filter
            end_time: Optional end time filter
            
        Returns:
            List of time series points
        """
        return self._position_storage.get_position_timeseries(user_id, venue, symbol, start_time, end_time)

    # Equity-related methods - delegate to EquityStorage
    def store_equity(self, equity: Equity) -> bool:
        """
        Store equity data in the state.
        
        Args:
            equity: The equity data to store
            
        Returns:
            bool: Whether the equity was stored in history
        """
        try:
            return self._equity_storage.store_equity(equity)
        except Exception as e:
            logger.error(f"Error storing equity: {e}")
            return False

    def get_equity(self, user_id: int, venue: str) -> Optional[Dict[str, Any]]:
        """
        Get equity data by user and venue.
        
        Args:
            user_id: The ID of the user
            venue: The venue
            
        Returns:
            Equity data if found, None otherwise
        """
        return self._equity_storage.get_equity(user_id, venue)

    def get_user_equity(self, user_id: int) -> Dict[str, Any]:
        """
        Get all equity data for a user.
        
        Args:
            user_id: The ID of the user
            
        Returns:
            Dictionary mapping venues to equity data
        """
        return self._equity_storage.get_user_equity(user_id)

    def get_venue_equity(self, venue: str) -> Dict[int, Any]:
        """
        Get all equity data for a venue.
        
        Args:
            venue: The venue
            
        Returns:
            Dictionary mapping user IDs to equity data
        """
        return self._equity_storage.get_venue_equity(venue)

    def get_equity_history(self, user_id: int, venue: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get equity history for a user and venue.
        
        Args:
            user_id: The ID of the user
            venue: The venue
            limit: Maximum number of history items to return
            
        Returns:
            List of equity history items
        """
        return self._equity_storage.get_equity_history(user_id, venue, limit)

    def get_equity_timeseries(self, user_id: int, venue: str,
                              start_time: Optional[datetime] = None,
                              end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Get equity time series data.
        
        Args:
            user_id: The ID of the user
            venue: The venue
            start_time: Optional start time filter
            end_time: Optional end time filter
            
        Returns:
            List of time series points
        """
        return self._equity_storage.get_equity_timeseries(user_id, venue, start_time, end_time)

    def get_equity_state(self) -> Dict[int, Dict[str, Any]]:
        """
        Get a copy of the entire equity state.
        
        Returns:
            Dictionary mapping user IDs to their equity data by venue
        """
        return self._equity_storage.get_all_equity()

    def get_latest_equity_snapshot(self, user_id: int, venue: str) -> Optional[Dict[str, Any]]:
        """
        Get the latest equity snapshot.
        
        Args:
            user_id: The ID of the user
            venue: The venue
            
        Returns:
            Dict with latest equity data or None if not found
        """
        return self._equity_storage.get_latest_equity_snapshot(user_id, venue)

    def get_equity_snapshot_at_time(self, user_id: int, venue: str, target_time: datetime) -> Optional[Dict[str, Any]]:
        """
        Get the equity snapshot closest to the specified time.
        
        Args:
            user_id: The ID of the user
            venue: The venue
            target_time: Target time to find the closest snapshot
            
        Returns:
            Dict with closest equity data or None if not found
        """
        return self._equity_storage.get_equity_snapshot_at_time(user_id, venue, target_time)

    def get_equity_timeseries_by_interval(self, user_id: int, venue: str, 
                                      interval: str = 'daily',
                                      start_time: Optional[datetime] = None,
                                      end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Get equity time series data aggregated by specified interval.
        
        Args:
            user_id: The ID of the user
            venue: The venue
            interval: Aggregation interval ('hourly', 'daily', 'weekly')
            start_time: Optional start time filter
            end_time: Optional end time filter
            
        Returns:
            List of time series points at specified interval
        """
        return self._equity_storage.get_equity_timeseries_by_interval(user_id, venue, interval, start_time, end_time)

    def clear(self) -> None:
        """Clear all state data."""
        with self._lock:
            # Clear job state
            self._job_storage.clear_all_job_data()

            # Clear position state
            self._position_storage.clear_all_position_data()

            # Clear equity state
            self._equity_storage.clear_all_equity_data()