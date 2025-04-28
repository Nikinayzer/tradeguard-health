"""
Position Evaluator

Checks position data for potentially risky patterns based on unrealized PnL.
"""
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta, timezone

from src.models import Job, Position
from src.models.risk_models import RiskCategory, Pattern
from src.models.position_models import PositionUpdateType
from src.risk.evaluators.base import BaseRiskEvaluator
from src.state.state_manager import StateManager
from src.utils.log_util import get_logger

logger = get_logger()


class PositionEvaluator(BaseRiskEvaluator):
    """Evaluates position data for potentially risky patterns"""

    def __init__(self, state_manager: StateManager):
        """
        Initialize the position evaluator.
        
        Args:
            state_manager: The state manager for accessing position data
        """
        super().__init__(
            evaluator_id="positions_evaluator",
            description="Checks positions for unrealized PnL thresholds"
        )
        self.state_manager = state_manager

        self.EARLY_PROFIT_THRESHOLD = 0.05
        self.LONG_HOLDING_DAYS_THRESHOLD = 7

    def evaluate(self, user_id: int, position_histories: Dict[str, List[Position]]) -> List[Pattern]:
        patterns = []
        patterns.extend(self.check_early_profit_exit(user_id, position_histories))
        patterns.extend(self.check_unrealized_pnl(user_id, position_histories))
        patterns.extend(self.check_long_holding_time(user_id, position_histories))
        return patterns

    def check_long_holding_time(self, user_id: int, position_histories: Dict[str, List[Position]]) -> List[Pattern]:
        """
        Check for positions held for too long based on first entry timestamp.
        
        Args:
            user_id: User ID being evaluated
            position_histories: Dictionary mapping position keys to lists of Position objects
            
        Returns:
            List of patterns for positions held for too long
        """
        patterns = []
        current_time = datetime.now(timezone.utc)

        for position_key, history in position_histories.items():
            if not history:
                continue

            current_position = history[0]
            # TODO shall skip?
            if current_position.qty == 0:
                continue

            # Find first entry timestamp by looking for earliest Increased event
            first_entry_time = None

            for position in reversed(history):  # Search from oldest to newest
                # We're interested in the first INCREASED event or SNAPSHOT
                # that marks the beginning of this position
                if (position.update_type == PositionUpdateType.INCREASED or
                        (position.update_type == PositionUpdateType.SNAPSHOT and first_entry_time is None)):
                    first_entry_time = position.timestamp
                    break

            # If we couldn't find an entry time, use the oldest timestamp in history
            if first_entry_time is None and history:
                # Take the last element which should be the oldest position in history
                first_entry_time = history[-1].timestamp

            # Skip if we still don't have a valid entry time
            if first_entry_time is None:
                continue

            # Calculate holding time
            holding_time = current_time - first_entry_time
            holding_days = holding_time.total_seconds() / (24 * 3600)

            # Check if holding time exceeds threshold
            if holding_days > self.LONG_HOLDING_DAYS_THRESHOLD:
                try:
                    venue, symbol = position_key.split('_', 1)
                except ValueError:
                    logger.warning(f"Invalid position key format: {position_key}")
                    venue = current_position.venue
                    symbol = current_position.symbol


                violation_ratio = holding_days / self.LONG_HOLDING_DAYS_THRESHOLD
                confidence = self.calculate_dynamic_confidence(
                    violation_ratio,
                )

                patterns.append(Pattern(
                    pattern_id="long_holding_time",
                    user_id=user_id,
                    message=f"Position on {symbol} held for {holding_days:.1f} days",
                    confidence=confidence,
                    category_weights={
                        RiskCategory.SUNK_COST: 0.6
                    },
                    details={
                        "position_key": position_key,
                        "symbol": symbol,
                        "venue": venue,
                        "holding_days": holding_days,
                        "threshold_days": self.LONG_HOLDING_DAYS_THRESHOLD,
                        "entry_time": first_entry_time.isoformat(),
                        "current_time": current_time.isoformat(),
                        "pnl": current_position.unrealized_pnl
                    }
                ))

        return patterns

    def check_unrealized_pnl(self, user_id: int, position_histories: Dict[str, List[Position]]) -> List[Pattern]:
        """
        Check positions for negative unrealized PnL exceeding thresholds.
        
        Args:
            user_id: User ID being evaluated
            position_histories: Dictionary mapping position keys to lists of Position objects
            
        Returns:
            List of patterns for positions with significant unrealized losses
        """
        patterns = []

        # For each position, check the most recent state (first item in history list)
        for position_key, history in position_histories.items():
            if not history:
                continue

            # TODO keep it like this?
            position = history[0]

            if position.unrealized_pnl >= 0:
                continue

            if position.usdt_amt == 0:
                continue

            pnl_percentage = (position.unrealized_pnl / position.usdt_amt) * 100

            if pnl_percentage >= -10:
                continue

            # PnL is worse than -10%, create a pattern
            # Calculate violation ratio (how much worse than threshold)
            # -15% is 1.5x worse than -10%
            violation_ratio = abs(pnl_percentage) / 10.0

            confidence = self.calculate_dynamic_confidence(
                violation_ratio,
            )

            try:
                venue, symbol = position_key.split('_', 1)
            except ValueError:
                logger.warning(f"Invalid position key format: {position_key}")
                venue = position.venue
                symbol = position.symbol

            patterns.append(Pattern(
                pattern_id="position_unrealized_pnl_threshold",
                user_id=user_id,
                message=f"Position has significant unrealized loss ({pnl_percentage:.2f}%)",
                confidence=confidence,
                category_weights={
                    RiskCategory.LOSS_BEHAVIOR: 0.7,
                    RiskCategory.SUNK_COST: 0.3
                },
                details={
                    "position_key": position_key,
                    "symbol": symbol,
                    "venue": venue,
                    "unrealized_pnl": position.unrealized_pnl,
                    "pnl_percentage": pnl_percentage,
                    "position_size": position.usdt_amt,
                    "threshold": -10.0
                }
            ))

        return patterns

    def check_early_profit_exit(self, user_id: int, position_histories: Dict[str, List[Position]]) -> List[Pattern]:
        """
        Identify instances where a trader exits profitable positions with small gains.
        Returns individual atomic patterns for each early exit instance.

        Args:
            user_id: User ID being evaluated
            position_histories: Dictionary mapping position keys to lists of Position objects

        Returns:
            List of atomic patterns for early profit exits
        """
        patterns = []

        for position_key, history in position_histories.items():
            if not history:
                continue

            try:
                venue, symbol = position_key.split('_', 1)
            except ValueError:
                logger.warning(f"Invalid position key format: {position_key}")
                continue

            for position in history:
                # Skip non-decrease events
                if position.update_type != PositionUpdateType.DECREASED:
                    continue

                entry_price = position.entry_price
                exit_price = position.mark_price

                side = position.side
                if side == 'Buy':
                    profit_pct = (exit_price - entry_price) / entry_price
                elif side == 'Sell':
                    profit_pct = (entry_price - exit_price) / entry_price
                else:
                    continue

                if 0 < profit_pct <= self.EARLY_PROFIT_THRESHOLD:
                    patterns.append(Pattern(
                        pattern_id="early_profit_exit",
                        user_id=user_id,
                        message=f"Early profit exit on {symbol} ({profit_pct:.2%})",
                        confidence=0.5,
                        category_weights={
                            RiskCategory.LOSS_BEHAVIOR: 1.0,
                        },
                        details={
                            "position_key": position_key,
                            "symbol": symbol,
                            "venue": venue,
                            "entry_price": entry_price,
                            "exit_price": exit_price,
                            "profit_pct": profit_pct
                        }
                    ))

        return patterns
