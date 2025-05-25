"""
Risk Evaluators Package

Contains evaluators for different risk patterns and behaviors.
"""

from src.risk.evaluators.base import BaseRiskEvaluator, RiskDataProvider
from src.risk.evaluators.user_limits import UserLimitsEvaluator
from src.risk.evaluators.trading_behavior import TradingBehaviorEvaluator
from src.risk.evaluators.positions_evaluator import PositionEvaluator
from typing import Dict
from src.state.state_manager import StateManager

__all__ = [
    'BaseRiskEvaluator',
    'UserLimitsEvaluator',
    'TradingBehaviorEvaluator',
    'PositionEvaluator',
    'create_evaluators'
]


def create_evaluators(state_manager: StateManager) -> Dict[str, BaseRiskEvaluator]:
    """
    Create all risk evaluators.
    
    Args:
        state_manager: State manager instance for accessing data
        
    Returns:
        Dictionary of evaluator instances keyed by their IDs
    """
    return {
        "user_limits": UserLimitsEvaluator(state_manager),
        "positions_evaluator": PositionEvaluator(state_manager),
    }
