"""
Equity Storage Manager

Provides in-memory storage for equity data.
Handles equity storage, retrieval, and time series tracking.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta, timezone
from threading import Lock

from src.models.equity_models import Equity
from src.utils.log_util import get_logger

logger = get_logger()


class EquityStorage:
    """
    Storage manager for equity data with in-memory storage.
    Provides methods to store, retrieve, and analyze equity data.
    """
    
    def __init__(self):
        """
        Initialize the equity storage with in-memory storage.

        """
        # In-memory storage
        self._equity_state = {}  # user_id -> venue -> Equity
        self._venue_equity = {}  # venue -> user_id -> Equity
        self._equity_history = {}  # user_id:venue -> [history_items]
        self._equity_timeseries = {}  # user_id:venue -> [(timestamp, wallet_balance, available_balance)]
        
        # Lock for thread safety in memory operations
        self._lock = Lock()
        
        logger.info("Equity storage initialized with in-memory storage only")
    
    def store_equity(self, equity: Equity) -> bool:
        """
        Store an equity update with appropriate indexing and history tracking.
        
        Args:
            equity: Equity object to store
            
        Returns:
            bool: True if stored in history, False if only current state was updated
        """
        return self._store_equity_in_memory(equity)
        
    def get_equity(self, user_id: int, venue: str) -> Optional[Dict[str, Any]]:
        """
        Get current equity state.
        
        Args:
            user_id: User ID
            venue: Trading venue 
            
        Returns:
            Dict with equity data or None if not found
        """
        with self._lock:
            equity = self._equity_state.get(user_id, {}).get(venue)
            
            if equity:
                if hasattr(equity, 'model_dump'):
                    return equity.model_dump()
                return equity.__dict__
            
            return None
        
    def get_user_equity(self, user_id: int) -> Dict[str, Any]:
        """
        Get all equity data for a user.
        
        Args:
            user_id: User ID
            
        Returns:
            Dictionary mapping venues to equity data
        """
        with self._lock:
            if user_id not in self._equity_state:
                return {}
                
            result = {}
            for venue, equity in self._equity_state[user_id].items():
                if hasattr(equity, 'model_dump'):
                    result[venue] = equity.model_dump()
                else:
                    result[venue] = equity.__dict__
                    
            return result
            
    def get_venue_equity(self, venue: str) -> Dict[int, Any]:
        """
        Get all equity data for a venue.
        
        Args:
            venue: Trading venue
            
        Returns:
            Dictionary mapping user IDs to equity data
        """
        with self._lock:
            if venue not in self._venue_equity:
                return {}
                
            result = {}
            for user_id, equity in self._venue_equity[venue].items():
                if hasattr(equity, 'model_dump'):
                    result[user_id] = equity.model_dump()
                else:
                    result[user_id] = equity.__dict__
                    
            return result
            
    def get_equity_history(self, user_id: int, venue: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get equity history.
        
        Args:
            user_id: User ID
            venue: Trading venue
            limit: Maximum number of history items to return
            
        Returns:
            List of equity history items
        """
        with self._lock:
            history_key = f"{user_id}:{venue}"
            
            if history_key in self._equity_history:
                history = []
                for item in self._equity_history[history_key][:limit]:
                    if hasattr(item, 'model_dump'):
                        history.append(item.model_dump())
                    else:
                        history.append(item.__dict__)
                return history
                
            return []
            
    def get_equity_timeseries(self, user_id: int, venue: str, 
                             start_time: Optional[datetime] = None, 
                             end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Get equity time series data.
        
        Args:
            user_id: User ID
            venue: Trading venue
            start_time: Optional start time filter
            end_time: Optional end time filter
            
        Returns:
            List of time series points
        """
        with self._lock:
            timeseries_key = f"{user_id}:{venue}"
            
            if timeseries_key not in self._equity_timeseries:
                return []
                
            # Start with all time series points
            timeseries = self._equity_timeseries[timeseries_key]
            
            # Apply time range filter if specified
            if start_time or end_time:
                filtered_timeseries = []
                
                for point in timeseries:
                    point_time = datetime.fromtimestamp(point['timestamp'] / 1000, tz=timezone.utc)
                    
                    if start_time and point_time < start_time:
                        continue
                        
                    if end_time and point_time > end_time:
                        continue
                        
                    filtered_timeseries.append(point)
                    
                return filtered_timeseries
                
            return timeseries
    
    def _store_equity_in_memory(self, equity: Equity) -> bool:
        """Store equity in memory."""
        with self._lock:
            user_id = equity.user_id
            venue = equity.venue
            
            # Initialize user's equity state if needed
            if user_id not in self._equity_state:
                self._equity_state[user_id] = {}
                
            # Initialize venue's equity state if needed
            if venue not in self._venue_equity:
                self._venue_equity[venue] = {}
                
            # Get previous state to check for changes
            prev_equity = self._equity_state.get(user_id, {}).get(venue)
            
            # Determine event type and whether to store in history
            store_in_history = False
            
            # First time seeing this equity - always store
            if not prev_equity:
                store_in_history = True
            else:
                try:
                    # Significant balance movement (>1%)
                    if (prev_equity.wallet_balance > 0 and
                            abs((equity.wallet_balance - prev_equity.wallet_balance) / prev_equity.wallet_balance) > 0.01):
                        store_in_history = True
                    
                    # Time-based sampling
                    else:
                        prev_time = prev_equity.timestamp
                        current_time = equity.timestamp
                        
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
            self._equity_state[user_id][venue] = equity
            self._venue_equity[venue][user_id] = equity
            
            # Store history if needed
            if store_in_history:
                # Create history key
                history_key = f"{user_id}:{venue}"
                
                # Initialize history list if needed
                if history_key not in self._equity_history:
                    self._equity_history[history_key] = []
                    
                # Add to front of list (most recent first)
                self._equity_history[history_key].insert(0, equity)
                
                # Trim list to keep only most recent 100 entries
                if len(self._equity_history[history_key]) > 100:
                    self._equity_history[history_key] = self._equity_history[history_key][:100]
                    
            # Update time series if significant or at 15-minute interval
            add_to_timeseries = store_in_history
            if not add_to_timeseries:
                # Check if we're at a 15-minute interval
                if equity.timestamp.minute % 15 == 0:
                    add_to_timeseries = True
                    
            if add_to_timeseries:
                # Create timeseries key
                timeseries_key = f"{user_id}:{venue}"
                
                # Initialize timeseries if needed
                if timeseries_key not in self._equity_timeseries:
                    self._equity_timeseries[timeseries_key] = []
                    
                # Add data point
                timestamp_ms = int(equity.timestamp.timestamp() * 1000)
                self._equity_timeseries[timeseries_key].append({
                    "timestamp": timestamp_ms,
                    "wallet_balance": equity.wallet_balance,
                    "available_balance": equity.available_balance
                })
                
                # Sort by timestamp
                self._equity_timeseries[timeseries_key].sort(key=lambda x: x["timestamp"])
                
                # Trim to 500 points max
                if len(self._equity_timeseries[timeseries_key]) > 500:
                    self._equity_timeseries[timeseries_key] = self._equity_timeseries[timeseries_key][-500:]
                    
            logger.debug(f"Stored equity for {venue} for user {user_id} in memory (history: {store_in_history})")
            return store_in_history
    
    def get_all_equity(self) -> Dict[int, Dict[str, Any]]:
        """
        Get a copy of the entire equity state.
        
        Returns:
            Dictionary mapping user IDs to their equity data by venue
        """
        all_equity = {}
        
        with self._lock:
            for user_id, venues in self._equity_state.items():
                user_equity = {}
                
                for venue, equity in venues.items():
                    if hasattr(equity, 'model_dump'):
                        user_equity[venue] = equity.model_dump()
                    else:
                        user_equity[venue] = equity.__dict__
                        
                all_equity[user_id] = user_equity
                
            return all_equity
            
    def clear_equity_data(self, user_id: Optional[int] = None, venue: Optional[str] = None) -> None:
        """
        Clear equity data, optionally filtered by user or venue.
        
        Args:
            user_id: Optional user ID to clear data for
            venue: Optional venue to clear data for
        """
        with self._lock:
            if user_id and venue:
                # Clear specific user+venue equity
                if user_id in self._equity_state and venue in self._equity_state[user_id]:
                    del self._equity_state[user_id][venue]
                    
                if venue in self._venue_equity and user_id in self._venue_equity[venue]:
                    del self._venue_equity[venue][user_id]
                    
                # Clear history and timeseries
                history_key = f"{user_id}:{venue}"
                if history_key in self._equity_history:
                    del self._equity_history[history_key]
                    
                if history_key in self._equity_timeseries:
                    del self._equity_timeseries[history_key]
                    
            elif user_id:
                # Clear all equity for user
                if user_id in self._equity_state:
                    # Remove from venue equity
                    for venue in list(self._equity_state[user_id].keys()):
                        if venue in self._venue_equity and user_id in self._venue_equity[venue]:
                            del self._venue_equity[venue][user_id]
                            
                    # Clear user's equity state
                    del self._equity_state[user_id]
                    
                # Clear history and timeseries
                prefix = f"{user_id}:"
                for key in list(self._equity_history.keys()):
                    if key.startswith(prefix):
                        del self._equity_history[key]
                        
                for key in list(self._equity_timeseries.keys()):
                    if key.startswith(prefix):
                        del self._equity_timeseries[key]
                        
            elif venue:
                # Clear all equity for venue
                if venue in self._venue_equity:
                    # Remove from user equity
                    for user_id in list(self._venue_equity[venue].keys()):
                        if user_id in self._equity_state and venue in self._equity_state[user_id]:
                            del self._equity_state[user_id][venue]
                            
                    # Clear venue's equity
                    del self._venue_equity[venue]
                    
                # Clear history and timeseries for venue
                for key in list(self._equity_history.keys()):
                    if f":{venue}" in key:
                        del self._equity_history[key]
                        
                for key in list(self._equity_timeseries.keys()):
                    if f":{venue}" in key:
                        del self._equity_timeseries[key]
                        
            else:
                # Clear all equity data
                self._equity_state.clear()
                self._venue_equity.clear()
                self._equity_history.clear()
                self._equity_timeseries.clear()
                
    def clear_all_equity_data(self) -> None:
        """Clear all equity data."""
        self.clear_equity_data()
        
    def get_latest_equity_snapshot(self, user_id: int, venue: str) -> Optional[Dict[str, Any]]:
        """
        Get the latest equity snapshot.
        
        Args:
            user_id: User ID
            venue: Trading venue
            
        Returns:
            Dict with latest equity data or None if not found
        """
        # Current state is already the latest snapshot
        return self.get_equity(user_id, venue)
        
    def get_equity_snapshot_at_time(self, user_id: int, venue: str, target_time: datetime) -> Optional[Dict[str, Any]]:
        """
        Get the equity snapshot closest to the specified time.
        
        Args:
            user_id: User ID
            venue: Trading venue
            target_time: Target time to find the closest snapshot
            
        Returns:
            Dict with closest equity data or None if not found
        """
        history = self.get_equity_history(user_id, venue, limit=100)
        if not history:
            return None
            
        # Find closest snapshot by time
        closest_snapshot = None
        min_time_diff = None
        
        for snapshot in history:
            if 'timestamp' in snapshot:
                try:
                    snapshot_time = datetime.fromisoformat(snapshot['timestamp'].replace('Z', '+00:00'))
                    time_diff = abs((snapshot_time - target_time).total_seconds())
                    
                    if min_time_diff is None or time_diff < min_time_diff:
                        min_time_diff = time_diff
                        closest_snapshot = snapshot
                except (ValueError, TypeError):
                    continue
                    
        return closest_snapshot
        
    def get_equity_timeseries_by_interval(self, user_id: int, venue: str, 
                                       interval: str = 'daily',
                                       start_time: Optional[datetime] = None,
                                       end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Get equity time series data aggregated by specified interval.
        
        Args:
            user_id: User ID
            venue: Trading venue
            interval: Aggregation interval ('hourly', 'daily', 'weekly')
            start_time: Optional start time filter
            end_time: Optional end time filter
            
        Returns:
            List of time series points at specified interval
        """
        # Get raw time series data
        raw_timeseries = self.get_equity_timeseries(user_id, venue, start_time, end_time)
        if not raw_timeseries:
            return []
            
        # Map for storing aggregated data
        aggregated_data = {}
        
        # Define time bucket key based on interval
        def get_bucket_key(dt):
            if interval == 'hourly':
                return dt.strftime('%Y-%m-%d %H:00:00')
            elif interval == 'daily':
                return dt.strftime('%Y-%m-%d')
            elif interval == 'weekly':
                # ISO week with year (e.g., 2023-W01)
                return f"{dt.isocalendar()[0]}-W{dt.isocalendar()[1]:02d}"
            else:
                # Default to daily
                return dt.strftime('%Y-%m-%d')
                
        # Process each data point
        for point in raw_timeseries:
            # Convert timestamp to datetime
            dt = datetime.fromtimestamp(point['timestamp'] / 1000, tz=timezone.utc)
            bucket_key = get_bucket_key(dt)
            
            # Initialize bucket if needed
            if bucket_key not in aggregated_data:
                aggregated_data[bucket_key] = {
                    'timestamp': dt,
                    'wallet_balance': 0.0,
                    'available_balance': 0.0,
                    'count': 0
                }
                
            # Update bucket with new data point
            aggregated_data[bucket_key]['wallet_balance'] += point['wallet_balance']
            aggregated_data[bucket_key]['available_balance'] += point['available_balance']
            aggregated_data[bucket_key]['count'] += 1
            
        # Calculate averages and format result
        result = []
        for bucket_key, data in aggregated_data.items():
            count = data['count']
            if count > 0:
                result.append({
                    'timestamp': data['timestamp'].isoformat(),
                    'wallet_balance': data['wallet_balance'] / count,
                    'available_balance': data['available_balance'] / count
                })
                
        # Sort by timestamp
        result.sort(key=lambda x: x['timestamp'])
        
        return result 