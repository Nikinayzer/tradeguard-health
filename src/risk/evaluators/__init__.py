"""
Risk Evaluators Package

Contains evaluators for different risk patterns and behaviors.
"""

from src.risk.evaluators.base import BaseRiskEvaluator
from src.risk.evaluators.overtrading import OverTradingEvaluator
from src.risk.evaluators.fomo import FOMOEvaluator
from src.risk.evaluators.sunk_cost import SunkCostEvaluator
from src.risk.evaluators.position_size import PositionSizeEvaluator
from src.risk.evaluators.time_pattern import TimePatternEvaluator
from src.risk.evaluators.user_limits import UserLimitsEvaluator
from src.risk.evaluators.batch_analyzer import UserBatchAnalyzer

__all__ = [
    'BaseRiskEvaluator',
    'OverTradingEvaluator',
    'FOMOEvaluator',
    'SunkCostEvaluator',
    'PositionSizeEvaluator',
    'TimePatternEvaluator',
    'UserLimitsEvaluator',
    'UserBatchAnalyzer',
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
        "overtrading": OverTradingEvaluator(),
        "fomo": FOMOEvaluator(),
        "sunk_cost": SunkCostEvaluator(),
        "position_size": PositionSizeEvaluator(),
        "time_pattern": TimePatternEvaluator(),
    }
    
    return evaluators 