"""
Position Storage Manager

Provides in-memory storage for position data with structured access patterns.
Handles position storage, retrieval, and time-series tracking.
"""

import json
import time
from typing import Dict, List, Any, Optional, Set, Tuple, Union
from datetime import datetime
from threading import Lock

from src.models.position_models import Position, PositionUpdateType
from src.utils.log_util import get_logger

logger = get_logger()


class PositionStorage:
    """
    Storage manager for position data using in-memory storage.
    Provides methods to store, retrieve, and analyze position data.
    """

    def __init__(self):
        """Initialize the position storage with in-memory storage."""
        self._positions_state = {}  # user_id -> position_key -> Position
        self._venue_positions = {}  # venue -> position_key -> Position
        self._position_history = {}  # user_id:venue_symbol -> [history_items]
        self._position_timeseries = {}  # user_id:venue_symbol -> [(timestamp, value)]

        # Lock for thread safety in memory operations
        self._lock = Lock()

        logger.info("Position storage initialized with in-memory storage")

    def store_position(self, position: Position) -> bool:
        """
        Store a position with appropriate indexing and history tracking.

        Args:
            position: Position object to store

        Returns:
            bool: True if stored in history, False if only current state was updated
        """
        return self._store_position_in_memory(position)

    def get_position(self, user_id: int, venue: str, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get current position state.

        Args:
            user_id: User ID
            venue: Trading venue
            symbol: Trading symbol

        Returns:
            Dict with position data or None if not found
        """
        with self._lock:
            position_key = f"{venue}_{symbol}"
            position = self._positions_state.get(user_id, {}).get(position_key)

            if position:
                return position.to_dict()

            return None

    def get_user_position_histories(self, user_id: int) -> Dict[str, List[Position]]:
        """
        Get position histories as lists of Position objects.
        
        Args:
            user_id: The user ID
            
        Returns:
            Dictionary mapping position keys to lists of Position objects
        """
        histories = {}
        
        # Get active positions for this user
        user_positions = self.get_user_positions(user_id)
        
        for position_key, _ in user_positions.items():
            try:
                venue, symbol = position_key.split('_', 1)
                
                # Get history as Position objects
                history_items = self.get_position_history(user_id, venue, symbol)
                
                # Store with position key
                histories[position_key] = history_items
            except ValueError:
                logger.warning(f"Invalid position key format: {position_key}")
        
        return histories

    def get_position_by_key(self, user_id: int, position_key: str) -> Optional[Dict[str, Any]]:
        """
        Get position by its key directly.

        Args:
            user_id: User ID
            position_key: Position key (venue_symbol)

        Returns:
            Dict with position data or None if not found
        """
        with self._lock:
            position = self._positions_state.get(user_id, {}).get(position_key)

            if position:
                return position.to_dict()

            return None

    def get_user_positions(self, user_id: int) -> Dict[str, Any]:
        """
        Get all positions for a user.

        Args:
            user_id: User ID

        Returns:
            Dictionary mapping position keys to positions
        """
        with self._lock:
            positions = {}
            user_positions = self._positions_state.get(user_id, {})

            for position_key, position in user_positions.items():
                positions[position_key] = position.to_dict()

            return positions

    def get_venue_positions(self, venue: str) -> Dict[str, Any]:
        """
        Get all positions for a venue.

        Args:
            venue: Trading venue

        Returns:
            Dictionary mapping position keys to positions
        """
        with self._lock:
            positions = {}
            venue_positions = self._venue_positions.get(venue, {})

            for position_key, position in venue_positions.items():
                positions[position_key] = position.to_dict()

            return positions

    def get_position_history(self, user_id: int, venue: str, symbol: str, limit: int = 100) -> List[Position]:
        """
        Get position history.

        Args:
            user_id: User ID
            venue: Trading venue
            symbol: Trading symbol
            limit: Maximum number of history items to return

        Returns:
            List of position history items, newest first
        """
        with self._lock:
            history_key = f"{user_id}:{venue}_{symbol}"

            if history_key in self._position_history:
                return self._position_history[history_key][:limit]

            return []

    def get_position_history_by_update_type(self, user_id: int, venue: str, symbol: str,
                                            update_types: List[Union[PositionUpdateType, str]], limit: int = 100) -> List[Position]:
        """
        Get position history filtered by update type.

        Args:
            user_id: User ID
            venue: Trading venue
            symbol: Trading symbol
            update_types: List of update types to include
            limit: Maximum number of history items to return

        Returns:
            List of position history items, newest first
        """
        # If no update type filter, just get all history
        if not update_types:
            return self.get_position_history(user_id, venue, symbol, limit)

        # Get full history and filter by update type
        history = self.get_position_history(user_id, venue, symbol, limit * 2)

        # Convert string update types to PositionUpdateType enum if needed
        update_type_values = []
        for t in update_types:
            if isinstance(t, str):
                try:
                    update_type_values.append(PositionUpdateType(t))
                except ValueError:
                    logger.warning(f"Invalid update type: {t}")
            else:
                update_type_values.append(t)

        # Filter
        filtered_history = [
            item for item in history
            if item.update_type in update_type_values
        ]

        return filtered_history[:limit]

    def get_position_timeseries(self, user_id: int, venue: str, symbol: str,
                                start_time: Optional[datetime] = None,
                                end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Get position time series data (PnL over time).

        Args:
            user_id: User ID
            venue: Trading venue
            symbol: Trading symbol
            start_time: Optional start time filter
            end_time: Optional end time filter

        Returns:
            List of time series points, oldest first
        """
        with self._lock:
            timeseries_key = f"{user_id}:{venue}_{symbol}"

            if timeseries_key not in self._position_timeseries:
                return []

            # Start with all time series points
            timeseries = self._position_timeseries[timeseries_key]

            # Apply time range filter if specified
            if start_time or end_time:
                min_score = int(start_time.timestamp() * 1000) if start_time else float('-inf')
                max_score = int(end_time.timestamp() * 1000) if end_time else float('inf')

                timeseries = [
                    item for item in timeseries
                    if min_score <= item['timestamp'] <= max_score
                ]

            # Return sorted by timestamp (oldest first)
            return sorted(timeseries, key=lambda x: x['timestamp'])

    def clear_position_data(self, user_id: Optional[int] = None, venue: Optional[str] = None) -> None:
        """
        Clear position data, optionally filtered by user or venue.

        Args:
            user_id: Optional user ID to clear data for
            venue: Optional venue to clear data for
        """
        with self._lock:
            if user_id and venue:
                # Clear specific user+venue positions
                if user_id in self._positions_state:
                    venue_prefix = f"{venue}_"
                    to_remove = []
                    for position_key in self._positions_state[user_id]:
                        if position_key.startswith(venue_prefix):
                            to_remove.append(position_key)

                    for position_key in to_remove:
                        del self._positions_state[user_id][position_key]

                if venue in self._venue_positions and user_id:
                    to_remove = []
                    for position_key, position in self._venue_positions[venue].items():
                        if position.user_id == user_id:
                            to_remove.append(position_key)

                    for position_key in to_remove:
                        del self._venue_positions[venue][position_key]

                # Clear history and timeseries
                prefix = f"{user_id}:{venue}_"
                to_remove = []
                for key in self._position_history:
                    if key.startswith(prefix):
                        to_remove.append(key)

                for key in to_remove:
                    del self._position_history[key]

                to_remove = []
                for key in self._position_timeseries:
                    if key.startswith(prefix):
                        to_remove.append(key)

                for key in to_remove:
                    del self._position_timeseries[key]

            elif user_id:
                # Clear all positions for user
                if user_id in self._positions_state:
                    # Remove from venue positions
                    for position_key, position in self._positions_state[user_id].items():
                        venue = position.venue
                        if venue and venue in self._venue_positions and position_key in self._venue_positions[venue]:
                            del self._venue_positions[venue][position_key]

                    # Clear user's positions
                    del self._positions_state[user_id]

                # Clear history and timeseries
                prefix = f"{user_id}:"
                to_remove = []
                for key in self._position_history:
                    if key.startswith(prefix):
                        to_remove.append(key)

                for key in to_remove:
                    del self._position_history[key]

                to_remove = []
                for key in self._position_timeseries:
                    if key.startswith(prefix):
                        to_remove.append(key)

                for key in to_remove:
                    del self._position_timeseries[key]

            elif venue:
                # Clear all positions for venue
                if venue in self._venue_positions:
                    # Remove from user positions
                    for position_key, position in self._venue_positions[venue].items():
                        user_id = position.user_id
                        if user_id and user_id in self._positions_state and position_key in self._positions_state[user_id]:
                            del self._positions_state[user_id][position_key]

                    # Clear venue's positions
                    del self._venue_positions[venue]

                # Clear history and timeseries for venue
                venue_prefix = f"{venue}_"
                to_remove = []
                for key in self._position_history:
                    parts = key.split(":")
                    if len(parts) > 1 and parts[1].startswith(venue_prefix):
                        to_remove.append(key)

                for key in to_remove:
                    del self._position_history[key]

                to_remove = []
                for key in self._position_timeseries:
                    parts = key.split(":")
                    if len(parts) > 1 and parts[1].startswith(venue_prefix):
                        to_remove.append(key)

                for key in to_remove:
                    del self._position_timeseries[key]

            else:
                # Clear all position data
                self._positions_state.clear()
                self._venue_positions.clear()
                self._position_history.clear()
                self._position_timeseries.clear()

    def clear_all_position_data(self) -> None:
        """Clear all position data."""
        self.clear_position_data()

    def get_all_positions(self) -> Dict[int, Dict[str, Any]]:
        """
        Get all positions grouped by user.

        Returns:
            Dictionary mapping user IDs to their position dictionaries
        """
        all_positions = {}

        with self._lock:
            for user_id, positions in self._positions_state.items():
                user_positions = {}

                for position_key, position in positions.items():
                    user_positions[position_key] = position.to_dict()

                all_positions[user_id] = user_positions

            return all_positions

    def _store_position_in_memory(self, position: Position) -> bool:
        """Store position in memory."""
        with self._lock:
            user_id = position.user_id
            venue = position.venue
            position_key = position.position_key

            # Initialize user's positions if needed
            if user_id not in self._positions_state:
                self._positions_state[user_id] = {}

            # Initialize venue's positions if needed
            if venue not in self._venue_positions:
                self._venue_positions[venue] = {}

            # Get previous state to check for changes
            prev_position = self._positions_state.get(user_id, {}).get(position_key)

            # Determine event type and whether to store in history
            store_in_history = False

            # Trading events always stored
            if position.update_type in [PositionUpdateType.INCREASED, PositionUpdateType.DECREASED, PositionUpdateType.CLOSED]:
                store_in_history = True

            # For snapshots, check if significant
            elif position.update_type == PositionUpdateType.SNAPSHOT:
                # First time seeing this position - always store
                if not prev_position:
                    store_in_history = True
                else:
                    # Check for significant changes
                    try:
                        # Significant price movement (>2%)
                        if prev_position.mark_price > 0 and abs(
                                (position.mark_price - prev_position.mark_price) / prev_position.mark_price) > 0.02:
                            store_in_history = True

                        # Significant PnL change (>10%)
                        elif (prev_position.unrealized_pnl != 0 and position.unrealized_pnl != 0 and
                              abs((position.unrealized_pnl - prev_position.unrealized_pnl) / abs(
                                  prev_position.unrealized_pnl)) > 0.1):
                            store_in_history = True

                        # Time-based sampling
                        else:
                            prev_time = prev_position.timestamp
                            current_time = position.timestamp

                            # Store hourly sample
                            if prev_time.hour != current_time.hour:
                                store_in_history = True

                            # Store daily sample
                            elif prev_time.day != current_time.day:
                                store_in_history = True

                    except (AttributeError, ValueError, ZeroDivisionError):
                        # If error in processing, treat as significant
                        store_in_history = True

            # Always update current state
            self._positions_state[user_id][position_key] = position
            self._venue_positions[venue][position_key] = position

            # Store history if needed
            if store_in_history:
                # Create history key
                history_key = f"{user_id}:{position_key}"

                # Initialize history list if needed
                if history_key not in self._position_history:
                    self._position_history[history_key] = []

                # Store a copy of the Position object to avoid reference issues
                position_copy = Position.from_dict(position.to_dict())
                
                # Add to front of list
                self._position_history[history_key].insert(0, position_copy)

                # Trim list to keep only most recent 100 entries
                if len(self._position_history[history_key]) > 100:
                    self._position_history[history_key] = self._position_history[history_key][:100]

            # Update time series if significant or at 15-minute interval
            add_to_timeseries = store_in_history
            if not add_to_timeseries:
                # Check if we're at a 15-minute interval
                if position.timestamp.minute % 15 == 0:
                    add_to_timeseries = True

            if add_to_timeseries:
                # Create timeseries key
                timeseries_key = f"{user_id}:{position_key}"

                # Initialize timeseries if needed
                if timeseries_key not in self._position_timeseries:
                    self._position_timeseries[timeseries_key] = []

                # Add data point
                timestamp_ms = int(position.timestamp.timestamp() * 1000)
                self._position_timeseries[timeseries_key].append({
                    "timestamp": timestamp_ms,
                    "value": position.unrealized_pnl
                })

                # Sort by timestamp
                self._position_timeseries[timeseries_key].sort(key=lambda x: x["timestamp"])

                # Trim to 500 points max
                if len(self._position_timeseries[timeseries_key]) > 500:
                    self._position_timeseries[timeseries_key] = self._position_timeseries[timeseries_key][-500:]

            logger.debug(f"Stored position {position_key} for user {user_id} in memory (history: {store_in_history})")
            return store_in_history