"""
Base Risk Evaluator

Provides the foundation class for all risk evaluators.
"""
from abc import abstractmethod
from typing import Dict, List, Any, Optional

from src.models import Job, Pattern
from src.utils.log_util import get_logger

logger = get_logger()


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

    def calculate_aggregated_confidence(self, triggers: List[Pattern]) -> float:
        """Weight triggers by their confidence (giving more weight to higher risks)."""
        if not triggers:
            return 0.0

        sorted_triggers = sorted(triggers, key=lambda t: t.confidence, reverse=True)
        # (1.0, 0.8, 0.6, 0.4, 0.2 for first 5)
        weights = [1.0 - (i * 0.2) for i in range(min(5, len(sorted_triggers)))]

        weighted_sum = sum(t.confidence * w for t, w in zip(sorted_triggers[:len(weights)], weights))
        return weighted_sum / sum(weights)