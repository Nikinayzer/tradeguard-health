"""
Risk Evaluators Package

Contains evaluators for different risk patterns and behaviors.
"""

from src.risk.evaluators.base import BaseRiskEvaluator
from src.risk.evaluators.user_limits import UserLimitsEvaluator
from src.risk.evaluators.trading_behavior import TradingBehaviorEvaluator
from src.risk.evaluators.positions_evaluator import PositionEvaluator

__all__ = [
    'BaseRiskEvaluator',
    'UserLimitsEvaluator',
    'TradingBehaviorEvaluator',
    'PositionEvaluator',
    'create_evaluators'
]


def create_evaluators():
    """
    Create and initialize all risk evaluators.
    
    Returns:
        Dictionary mapping evaluator IDs to evaluator instances
    """
    from src.state.state_manager import StateManager
    state_manager = StateManager()
    
    evaluators = {
        "user_limits": UserLimitsEvaluator(),
        "position_behavior": TradingBehaviorEvaluator(),
        "positions_evaluator": PositionEvaluator(state_manager),
    }

    return evaluators
