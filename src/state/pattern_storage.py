"""
Pattern Storage

Manages storage and retrieval of risk evaluation patterns per user.
"""
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Dict, List, Optional

from src.models.risk_models import AtomicPattern, CompositePattern
from src.utils.log_util import get_logger

logger = get_logger()


class PatternStorage:
    """In-memory storage for risk evaluation patterns."""

    def __init__(self):
        """Initialize pattern storage."""
        self._patterns: Dict[int, List[AtomicPattern]] = {}  # user_id -> patterns
        self._composite_patterns: Dict[int, List[CompositePattern]] = {}  # user_id -> composite patterns
        self._lock = Lock()

    def store_patterns(self, user_id: int, patterns: List[AtomicPattern]) -> None:
        """
        Store patterns for a user.
        For unique patterns:
        - If pattern has job_id, overwrites existing ones with same pattern_id AND job_id
        - If pattern has positions_key, overwrites existing ones with same pattern_id AND positions_key
        - If pattern has no positions_key, overwrites existing ones with same pattern_id
        For non-unique patterns, always adds them to the list.

        Args:
            user_id: User ID
            patterns: List of patterns to store
        """
        with self._lock:
            existing_patterns = self._patterns.get(user_id, [])

            unique_patterns = [p for p in patterns if p.unique]
            non_unique_patterns = [p for p in patterns if not p.unique]

            if unique_patterns:
                deduplicated_unique = {}
                for pattern in unique_patterns:
                    # For patterns with job_id, use pattern_id and job_id for deduplication
                    if pattern.job_id:
                        key = (pattern.pattern_id, tuple(sorted(pattern.job_id)))
                    else:
                        # For patterns without job_id, use existing position-based logic
                        key = (pattern.pattern_id, pattern.position_key)

                    if key not in deduplicated_unique or pattern.start_time > deduplicated_unique[key].start_time:
                        deduplicated_unique[key] = pattern

                new_unique_keys = set(deduplicated_unique.keys())

                # Keep existing patterns that are either:
                # 1 Not unique
                # 2 Unique but with different pattern_id
                # 3 Unique with same pattern_id but different job_id/position_key
                existing_patterns = [
                    p for p in existing_patterns
                    if not p.unique or
                       (
                               (p.pattern_id,
                                tuple(sorted(p.job_id)) if p.job_id else p.position_key)
                               not in new_unique_keys
                       )
                ]

                existing_patterns.extend(deduplicated_unique.values())

            existing_patterns.extend(non_unique_patterns)

            self._patterns[user_id] = existing_patterns
            self._clear_old_patterns()

            logger.debug(f"Stored {len(patterns)} patterns for user {user_id} "
                         f"({len(unique_patterns)} unique, {len(non_unique_patterns)} non-unique)")
            logger.debug(f"Total patterns in storage for user {user_id}: {len(existing_patterns)}")
            for pattern in existing_patterns:
                logger.debug(f"Pattern: {pattern.pattern_id} "
                             f"(unique={pattern.unique}, "
                             f"positions_key={pattern.position_key}, "
                             f"job_id={pattern.job_id})")

    def store_composite_patterns(self, user_id: int, patterns: List[CompositePattern]) -> None:
        """
        Store composite patterns for a user.
        
        Args:
            user_id: User ID
            patterns: List of composite patterns to store
        """
        with self._lock:
            if user_id not in self._composite_patterns:
                self._composite_patterns[user_id] = []
            self._composite_patterns[user_id].extend(patterns)

            self._clear_old_patterns()

            logger.debug(f"Stored {len(patterns)} composite patterns for user {user_id}")

    def _clear_old_patterns(self) -> None:
        """
        Clear patterns that are no longer active based on their TTL.
        Uses the is_active property from BasePattern to determine if a pattern should be kept.
        """
        for user_id in list(self._patterns.keys()):
            self._patterns[user_id] = [
                pattern for pattern in self._patterns[user_id]
                if pattern.is_active
#                or (isinstance(pattern, AtomicPattern) and pattern.consumed) # todo just make dump to db instead
            ]
            if not self._patterns[user_id]:
                del self._patterns[user_id]

        for user_id in list(self._composite_patterns.keys()):
            self._composite_patterns[user_id] = [
                pattern for pattern in self._composite_patterns[user_id]
                if pattern.is_active
            ]
            if not self._composite_patterns[user_id]:
                del self._composite_patterns[user_id]

        logger.debug("Cleared expired patterns based on TTL")

    def get_user_patterns(self, user_id: int, hours: int = 24) -> List[AtomicPattern]:
        """
        Get active patterns for a user within timeframe.
        
        Args:
            user_id: User ID
            hours: Number of hours to look back
            
        Returns:
            List of active patterns within timeframe
        """
        try:
            logger.info(f"[PatternStorage] Getting patterns for user {user_id} (hours={hours})")
            with self._lock:
                if user_id not in self._patterns:
                    logger.info(f"[PatternStorage] No patterns found for user {user_id}")
                    return []

                cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
                logger.info(f"[PatternStorage] Cutoff time: {cutoff_time}")

                patterns = []
                for pattern in self._patterns[user_id]:
                    if not pattern.is_active:
                        continue

                    if pattern.end_time:
                        if pattern.start_time <= cutoff_time <= pattern.end_time:
                            patterns.append(pattern)
                        elif pattern.start_time >= cutoff_time:
                            patterns.append(pattern)
                    else:
                        patterns.append(pattern)
                
                logger.info(f"[PatternStorage] Found {len(patterns)} active patterns within timeframe")
                for pattern in patterns:
                    logger.debug(f"[PatternStorage] Pattern: {pattern.pattern_id} (start_time={pattern.start_time}, end_time={pattern.end_time}, is_active={pattern.is_active})")
                
                return patterns
        except Exception as e:
            logger.error(f"[PatternStorage] Error getting patterns for user {user_id}: {str(e)}", exc_info=True)
            return []

    def get_user_composite_patterns(self, user_id: int, hours: int = 24) -> List[CompositePattern]:
        """
        Get active composite patterns for a user within timeframe.
        
        Args:
            user_id: User ID
            hours: Number of hours to look back
            
        Returns:
            List of active composite patterns within timeframe
        """
        with self._lock:
            if user_id not in self._composite_patterns:
                return []

            cutoff_time = datetime.now() - timedelta(hours=hours)
            return [
                pattern for pattern in self._composite_patterns[user_id]
                if pattern.start_time >= cutoff_time and pattern.is_active
            ]

    def clear_user_patterns(self, user_id: int) -> None:
        """
        Clear all patterns for a user.
        
        Args:
            user_id: User ID
        """
        with self._lock:
            if user_id in self._patterns:
                del self._patterns[user_id]
            if user_id in self._composite_patterns:
                del self._composite_patterns[user_id]
            logger.debug(f"Cleared patterns for user {user_id}")

    def clear_all_patterns(self) -> None:
        """Clear all patterns."""
        with self._lock:
            self._patterns.clear()
            self._composite_patterns.clear()
            logger.debug("Cleared all patterns")
