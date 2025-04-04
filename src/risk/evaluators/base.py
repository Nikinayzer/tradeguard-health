"""
Base Risk Evaluator

Provides the foundation class for all risk evaluators.
"""
import math
from abc import abstractmethod
from datetime import datetime
from typing import Dict, List, Any, Optional

from src.models import Job, Pattern
from src.utils.log_util import get_logger

logger = get_logger()


def apply_confidence_decay(confidence: float,
                           event_time: datetime,
                           current_time: Optional[datetime] = None,
                           half_life_minutes: int = 120,
                           ) -> float:
    """
    Apply time-based decay to a confidence value.

    Uses exponential decay formula: confidence * (0.5)^(time/half_life)

    Args: confidence: Original confidence value (0.0-1.0) event_time: When the event occurred current_time: Current
    time (defaults to now) half_life_minutes: Minutes after which confidence is halved.
     30-60 for Rapid Decay (FOMO, Panic),
     120-240 for Intra-Day Patterns,
     720-1440 for Daily Patterns

    Returns:
        Decayed confidence value
    """
    if confidence <= 0 or not event_time:
        return confidence

    if current_time is None:
        current_time = datetime.now()

    time_diff = (current_time - event_time).total_seconds() / 60.0
    if time_diff < 0:
        return confidence

    decay_factor = math.pow(0.5, time_diff / half_life_minutes)
    decayed_confidence = confidence * decay_factor

    return decayed_confidence


def calculate_dynamic_confidence(
        violation_ratio: float,
        base: float = 0.6,
        scaling: float = 0.1,
        boost: Optional[float] = 0.0,
) -> float:
    """
    Calculates confidence based on a violation ratio using a log scale.

    Args:
        violation_ratio: How much the threshold was exceeded (e.g. 1.5 = 50% over).
        base: Base confidence to start from.
        scaling: How fast confidence grows.
        boost: Optional extra boost from context (e.g. recent losses)

    Returns:
        Float confidence between 0 and max_confidence
    """
    adjusted = base + scaling * math.log1p((violation_ratio - 1) * 10) + boost
    return min(1.0, adjusted)


class BaseRiskEvaluator:
    """Base class for all risk evaluators"""

    @abstractmethod
    def __init__(self, evaluator_id: str, description: str):
        """
        Initialize the base evaluator.
        
        Args:
            evaluator_id: Unique identifier for this evaluator
            description: Description of what this evaluator analyzes
        """
        self.evaluator_id = evaluator_id
        self.description = description

    @abstractmethod
    def evaluate(self, user_id: int, job_history: Dict[int, Job]) -> List[Pattern]:
        """
        Evaluate risk for a job.
        
        Args:
            user_id: User ID
            job_history: User's job history as a dictionary mapping job_id to Job objects
            
        Returns:
            List of evidence dictionaries, each containing:
                - category_id: Risk category ID
                - confidence: Confidence level (0-1)
                - data: Additional evidence data
        """
        raise NotImplementedError("Subclasses must implement evaluate()")
