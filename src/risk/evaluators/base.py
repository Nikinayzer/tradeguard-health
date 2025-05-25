"""
Base Risk Evaluator

Provides the foundation class for all risk evaluators.
"""
import math
from abc import abstractmethod
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Protocol

from src.models import Job, Position, Equity, AtomicPattern, CompositePattern
from src.state.state_manager import StateManager
from src.utils.log_util import get_logger

logger = get_logger()


class RiskDataProvider(Protocol):
    """Interface for providing access to state management."""
    @property
    def job_storage(self):
        """Get job storage instance."""
        ...

    @property
    def position_storage(self):
        """Get position storage instance."""
        ...

    @property
    def equity_storage(self):
        """Get equity storage instance."""
        ...

    @property
    def pattern_storage(self):
        """Get pattern storage instance."""
        ...


class BaseRiskEvaluator:
    """Base class for all risk evaluators"""

    def __init__(self, evaluator_id: str, description: str, state_manager: StateManager):
        """
        Initialize the base evaluator.
        
        Args:
            evaluator_id: Unique identifier for this evaluator
            description: Description of what this evaluator analyzes
            state_manager: State manager instance for accessing data
        """
        self.evaluator_id = evaluator_id
        self.description = description
        self.state_manager = state_manager

    @abstractmethod
    def evaluate(self, user_id: int) -> List[AtomicPattern]:
        """
        Evaluate risk for a user.
        
        Args:
            user_id: User ID to evaluate
            
        Returns:
            List of detected risk patterns
        """
        raise NotImplementedError("Subclasses must implement evaluate()")

    def apply_confidence_decay(self,
                               confidence: float,
                               event_time: datetime,
                               current_time: Optional[datetime] = None,
                               half_life_minutes: int = 120,
                               ) -> float:
        """
        Apply time-based decay to a confidence value.

        Uses exponential decay formula: confidence * (0.5)^(time/half_life)

        Args: 
            confidence: Original confidence value (0.0-1.0) 
            event_time: When the event occurred 
            current_time: Current time (defaults to now) 
            half_life_minutes: Minutes after which confidence is halved.
             30-60 for Rapid Decay (FOMO, Panic),
             120-240 for Intra-Day Patterns,
             720-1440 for Daily Patterns

        Returns:
            Decayed confidence value
        """
        if confidence <= 0 or not event_time:
            return confidence

        if current_time is None:
            current_time = datetime.now(timezone.utc)

        time_diff = (current_time - event_time).total_seconds() / 60.0
        if time_diff < 0:
            return confidence

        decay_factor = math.pow(0.5, time_diff / half_life_minutes)
        decayed_confidence = confidence * decay_factor

        return decayed_confidence

    @classmethod
    def calculate_dynamic_severity(cls, violation_ratio: float, max_confidence: float = 1.0) -> float:
        """
        Calculates confidence based purely on the violation ratio using a logarithmic scale.
        The confidence reaches 1.0 when violation_ratio equals 2.

        Args:
            violation_ratio: How much the threshold was exceeded (e.g. 1.5 = 50% over).
            max_confidence: The maximum confidence, usually 1.0.

        Returns:
            Float confidence between 0 and max_confidence.
        """
        # Ensure violation_ratio is at least 1 to avoid math domain error
        effective_ratio = max(1.0, violation_ratio)

        adjusted = math.log1p((effective_ratio - 1) * 10)
        # Normalize
        normalized_confidence = min(max_confidence, adjusted / math.log1p(10))

        return normalized_confidence
