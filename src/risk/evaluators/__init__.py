"""
Risk Evaluators Package

Contains evaluators for different risk patterns and behaviors.
"""

from src.risk.evaluators.base import BaseRiskEvaluator
from src.risk.evaluators.user_limits import UserLimitsEvaluator
from src.risk.evaluators.trading_behavior import TradingBehaviorEvaluator

__all__ = [
    'BaseRiskEvaluator',
    'UserLimitsEvaluator',
    'TradingBehaviorEvaluator',
    'create_evaluators'
]


def create_evaluators():
    """
    Create and initialize all risk evaluators.
    
    Returns:
        Dictionary mapping evaluator IDs to evaluator instances
    """
    evaluators = {
        "user_limits": UserLimitsEvaluator(),
        "position_behavior": TradingBehaviorEvaluator(),
    }

    return evaluators
