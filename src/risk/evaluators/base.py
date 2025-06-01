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

    @classmethod
    def calculate_dynamic_severity(
        cls,
        violation_ratio: float,
        max_violation: float = 2.0,
        inverted: bool = False
        ) -> float:
        """
        Calculate normalized severity based on violation ratio.

        If `inverted=True`, then lower ratios are treated as worse (e.g. early cooldowns).
        The function always returns a value in [0.0, 1.0].

        :param violation_ratio: observed/expected ratio
        :param max_violation: the ratio that maps to severity = 1.0
        :param inverted: if True, treat smaller values as higher severity
        :return: severity score within [0.0, 1.0]
        """
        EPSILON = 1e-6

        if violation_ratio <= 0:
            return 0.0

        if inverted:
            adjusted_ratio = max_violation / max(violation_ratio, EPSILON)
        else:
            adjusted_ratio = min(violation_ratio, max_violation)

        score = math.log2(adjusted_ratio) / math.log2(max_violation)

        return round(min(max(score, 0.0), 1.0), 2)
