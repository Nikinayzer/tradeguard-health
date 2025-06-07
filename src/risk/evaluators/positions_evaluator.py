"""
Positions Evaluator

Evaluates position-related risk patterns.
"""
from typing import Dict, List
from datetime import datetime, timezone

from src.models import Position, AtomicPattern, RiskCategory
from src.models.position_models import PositionUpdateType
from src.risk.evaluators.base import BaseRiskEvaluator
from src.state.state_manager import StateManager
from src.utils.log_util import get_logger
from src.market.market_data_service import MarketDataService, KlineInterval
from src.market.trends_service import TrendsService

logger = get_logger()


class PositionEvaluator(BaseRiskEvaluator):
    """Evaluates position-related risk patterns."""

    def __init__(self, state_manager: StateManager):
        """Initialize the evaluator."""
        super().__init__(
            evaluator_id="positions_evaluator",
            description="Evaluates position-related risk patterns",
            state_manager=state_manager
        )

        self.EARLY_PROFIT_THRESHOLD = 0.05
        self.LONG_HOLDING_DAYS_THRESHOLD = 5 # todo change to 7
        self.VOLATILITY_THRESHOLD = 0.5  # 50% annualized volatility
        self.LIQUIDITY_THRESHOLD = 0.02  # 2% spread threshold
        self.MIN_LIQUIDITY_DEPTH = 100000  # Minimum depth
        self.HYPE_THRESHOLD = 0.7  # Minimum hype score to consider a coin hyped (0.0-1.0)
        self.RECENT_SPIKE_THRESHOLD = 0.5  # Minimum recent spike score to consider a coin spiking (0.0-1.0)
        self.market_data = MarketDataService()
        self.trends_service = TrendsService()

    async def evaluate(self, user_id: int) -> List[AtomicPattern]:
        """
        Evaluate position data for risk patterns.
        
        Args:
            user_id: User ID to evaluate
            
        Returns:
            List of detected risk patterns
        """
        try:
            logger.info(f"[PositionEvaluator] Starting evaluation for user {user_id}")
            positions = self.state_manager.position_storage.get_user_position_histories(user_id)
            if not positions:
                logger.info(f"[PositionEvaluator] No position history found for user {user_id}")
                return []

            patterns = []

            try:
                logger.debug("[PositionEvaluator] Running early profit exit check...")
                patterns.extend(self.check_early_profit_exit(user_id, positions))
            except Exception as e:
                logger.error(f"[PositionEvaluator] Error in early profit exit check: {str(e)}")

            try:
                logger.debug("[PositionEvaluator] Running unrealized PnL check...")
                patterns.extend(self.check_unrealized_pnl(user_id, positions))
            except Exception as e:
                logger.error(f"[PositionEvaluator] Error in unrealized PnL check: {str(e)}")

            try:
                logger.debug("[PositionEvaluator] Running long holding time check...")
                patterns.extend(self.check_long_holding_time(user_id, positions))
            except Exception as e:
                logger.error(f"[PositionEvaluator] Error in long holding time check: {str(e)}")

            try:
                logger.debug("[PositionEvaluator] Running volatility risk check...")
                patterns.extend(await self.check_volatility_risk(user_id, positions))
            except Exception as e:
                logger.error(f"[PositionEvaluator] Error in volatility risk check: {str(e)}")

            try:
                logger.debug("[PositionEvaluator] Running liquidity risk check...")
                patterns.extend(await self.check_liquidity_risk(user_id, positions))
            except Exception as e:
                logger.error(f"[PositionEvaluator] Error in liquidity risk check: {str(e)}")

            try:
                logger.debug("[PositionEvaluator] Running hype check...")
                patterns.extend(await self.check_coin_hype(user_id, positions))
            except Exception as e:
                logger.error(f"[PositionEvaluator] Error in hype check: {str(e)}")

            logger.info(f"[PositionEvaluator] Evaluation complete. Found {len(patterns)} patterns")
            return patterns

        except Exception as e:
            logger.error(f"[PositionEvaluator] Error in evaluate: {str(e)}")
            raise

    def check_long_holding_time(self, user_id: int, position_histories: Dict[str, List[Position]]) -> List[
                                AtomicPattern]:
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
            if current_position.qty == 0:
                continue

            first_entry_time = None

            for position in reversed(history):
                if (position.update_type == PositionUpdateType.INCREASED or
                        (position.update_type == PositionUpdateType.SNAPSHOT and first_entry_time is None)):
                    first_entry_time = position.timestamp
                    break

            if first_entry_time is None and history:
                first_entry_time = history[-1].timestamp

            if first_entry_time is None:
                continue

            holding_time = current_time - first_entry_time
            holding_days = holding_time.total_seconds() / (24 * 3600)
            logger.error(f"first_entry_time: {first_entry_time}")
            logger.error(f"current_time: {current_time}")
            logger.error(f"holding_time: {holding_time}")
            logger.error(f"holding days: {holding_days}")
            if holding_days > self.LONG_HOLDING_DAYS_THRESHOLD:
                try:
                    venue, symbol = position_key.split('_', 1)
                except ValueError:
                    logger.warning(f"Invalid position key format: {position_key}")
                    venue = current_position.venue
                    symbol = current_position.symbol

                violation_ratio = holding_days / self.LONG_HOLDING_DAYS_THRESHOLD
                severity = self.calculate_dynamic_severity(
                    violation_ratio,
                )

                message = f"Position {position_key} held for {holding_days:.1f} days"
                description = (
                    f"Position has been open for {holding_days:.1f} days, exceeding the {self.LONG_HOLDING_DAYS_THRESHOLD} day threshold. "
                    f"Current unrealized PnL: {current_position.unrealized_pnl:.2f} USDT. "
                    "Consider reviewing position exit strategy."
                )

                patterns.append(AtomicPattern(
                    pattern_id="position_long_holding_time",
                    user_id=user_id,
                    position_key=position_key,
                    message=message,
                    description=description,
                    severity=severity,
                    unique=True,
                    ttl_minutes=60 * 24 * 7,
                    details={
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

    def check_unrealized_pnl(self, user_id: int, position_histories: Dict[str, List[Position]]) -> List[AtomicPattern]:
        """
        Check positions for negative unrealized PnL exceeding thresholds.
        
        Args:
            user_id: User ID being evaluated
            position_histories: Dictionary mapping position keys to lists of Position objects
            
        Returns:
            List of patterns for positions with significant unrealized losses
        """
        patterns = []

        for position_key, history in position_histories.items():
            if not history:
                continue

            position = history[0]

            if position.unrealized_pnl >= 0:
                continue

            if position.usdt_amt == 0:
                continue

            pnl_percentage = (position.unrealized_pnl / position.usdt_amt) * 100

            if pnl_percentage >= -10:
                continue

            violation_ratio = abs(pnl_percentage) / 10.0
            severity = self.calculate_dynamic_severity(
                violation_ratio,
            )

            try:
                venue, symbol = position_key.split('_', 1)
            except ValueError:
                logger.warning(f"Invalid position key format: {position_key}")
                venue = position.venue
                symbol = position.symbol

            message = f"Position {position_key} has significant unrealized loss ({pnl_percentage:.2f}%)"
            description = (
                f"Position has unrealized loss of {pnl_percentage:.2f}% ({position.unrealized_pnl:.2f} USDT). "
                f"Position size: {position.usdt_amt:.2f} USDT. "
                "Consider reviewing stop-loss strategy and position sizing."
            )

            patterns.append(AtomicPattern(
                pattern_id="position_unrealized_pnl_threshold",
                user_id=user_id,
                position_key=position_key,
                message=message,
                description=description,
                severity=severity,
                unique=True,
                ttl_minutes=60 * 24 * 7,
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

    def check_early_profit_exit(self, user_id: int, position_histories: Dict[str, List[Position]]) -> List[
                                AtomicPattern]:
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
                    message = f"Early profit exit on {symbol} ({profit_pct:.2%})"
                    description = (
                        f"Position was closed with a small profit of {profit_pct:.2%}. "
                        f"Entry price: {entry_price:.2f}, Exit price: {exit_price:.2f}. "
                        "Consider reviewing take-profit strategy to maximize gains."
                    )

                    patterns.append(AtomicPattern(
                        pattern_id="position_early_profit_exit",
                        user_id=user_id,
                        position_key=position_key,
                        message=message,
                        description=description,
                        severity=1.0,
                        ttl_minutes=60 * 24 * 7,
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

    def check_position_size_relative_to_equity(self, user_id: int) -> List[AtomicPattern]:
        """
        Check if position sizes are too large relative to user's equity.

        Args:
            user_id: User ID to check

        Returns:
            List of patterns for positions that exceed equity thresholds
        """
        patterns = []

        positions = self.state_manager.position_storage.get_user_positions(user_id)
        if not positions:
            return patterns

        equity_data = self.state_manager.equity_storage.get_user_equity(user_id)
        if not equity_data:
            return patterns

        total_equity = sum(
            equity['wallet_balance'] + equity['total_unrealized_pnl']
            for equity in equity_data.values()
        )

        if total_equity <= 0:
            return patterns

        for position_key, position in positions.items():
            try:
                venue, symbol = position_key.split('_', 1)

                position_size = position['usdt_amt']
                position_equity_ratio = position_size / total_equity
                severity = position_equity_ratio

                WARNING_THRESHOLD = 0.20
                CRITICAL_THRESHOLD = 0.40

                if position_equity_ratio >= CRITICAL_THRESHOLD:
                    message = f"Position {position_key} size ({position_equity_ratio:.1%} of equity) exceeds critical threshold"
                elif position_equity_ratio >= WARNING_THRESHOLD:
                    message = f"Position {position_key} size ({position_equity_ratio:.1%} of equity) exceeds warning threshold"
                else:
                    continue

                pattern = AtomicPattern(
                    pattern_id="position_size_equity_ratio",
                    message=message,
                    severity=severity,
                    unique=True,
                    ttl_minutes=60 * 24,
                    positions_key=position_key,
                    details={
                        "position_size_usdt": position_size,
                        "total_equity_usdt": total_equity,
                        "equity_ratio": position_equity_ratio,
                        "threshold_exceeded": "critical" if position_equity_ratio >= CRITICAL_THRESHOLD else "warning",
                        "venue": venue,
                        "symbol": symbol
                    }
                )
                patterns.append(pattern)

            except (ValueError, KeyError) as e:
                logger.error(f"Error processing position {position_key}: {str(e)}")
                continue

        return patterns

    async def check_volatility_risk(self, user_id: int, position_histories: Dict[str, List[Position]]) -> List[
                                    AtomicPattern]:
        """
        Check positions for high volatility risk.
        
        Args:
            user_id: User ID being evaluated
            position_histories: Dictionary mapping position keys to lists of Position objects
            
        Returns:
            List of patterns for positions in high volatility markets
        """
        patterns = []

        for position_key, history in position_histories.items():
            if not history:
                continue

            current_position = history[0]
            if current_position.qty == 0:
                continue

            try:
                venue, symbol = position_key.split('_', 1)
            except ValueError:
                logger.warning(f"Invalid position key format: {position_key}")
                continue

            volatility = await self.market_data.get_volatility(symbol, KlineInterval.DAY)
            if volatility is None:
                continue

            if volatility > self.VOLATILITY_THRESHOLD:
                violation_ratio = volatility / self.VOLATILITY_THRESHOLD
                severity = self.calculate_dynamic_severity(violation_ratio, max_violation=4.0)

                message = f"Position {position_key} in high volatility market ({volatility:.1%})"
                description = (
                    f"Market volatility ({volatility:.1%}) exceeds threshold ({self.VOLATILITY_THRESHOLD:.1%}). "
                    f"Position size: {current_position.usdt_amt:.2f} USDT, Unrealized PnL: {current_position.unrealized_pnl:.2f} USDT. "
                    "Consider reducing position size or implementing tighter stop-loss."
                )

                patterns.append(AtomicPattern(
                    pattern_id="position_high_volatility",
                    user_id=user_id,
                    position_key=position_key,
                    message=message,
                    description=description,
                    severity=severity,
                    unique=True,
                    ttl_minutes=60 * 24,
                    details={
                        "position_key": position_key,
                        "symbol": symbol,
                        "venue": venue,
                        "volatility": volatility,
                        "threshold": self.VOLATILITY_THRESHOLD,
                        "position_size": current_position.usdt_amt,
                        "unrealized_pnl": current_position.unrealized_pnl
                    }
                ))

        return patterns

    async def check_liquidity_risk(self, user_id: int, position_histories: Dict[str, List[Position]]) -> List[
                                    AtomicPattern]:
        """
        Check positions for liquidity risk based on market depth and spread.
        
        Args:
            user_id: User ID being evaluated
            position_histories: Dictionary mapping position keys to lists of Position objects
            
        Returns:
            List of patterns for positions in low liquidity markets
        """
        patterns = []

        for position_key, history in position_histories.items():
            if not history:
                continue

            current_position = history[0]
            if current_position.qty == 0:
                continue

            try:
                venue, symbol = position_key.split('_', 1)
            except ValueError:
                logger.warning(f"Invalid position key format: {position_key}")
                continue

            liquidity_metrics = await self.market_data.get_liquidity_metrics(symbol)
            if liquidity_metrics is None:
                continue

            spread = liquidity_metrics.get('spread', 0)
            depth = liquidity_metrics.get('depth', 0)

            position_size = current_position.usdt_amt
            depth_ratio = position_size / depth if depth > 0 else float('inf')

            if spread > self.LIQUIDITY_THRESHOLD:
                violation_ratio = spread / self.LIQUIDITY_THRESHOLD
                severity = self.calculate_dynamic_severity(violation_ratio, max_violation=4.0)

                message = f"Position {position_key} in high spread market ({spread:.2%})"
                description = (
                    f"Market spread ({spread:.2%}) exceeds threshold ({self.LIQUIDITY_THRESHOLD:.2%}). "
                    f"Position size: {position_size:.2f} USDT, Market depth: {depth:.2f} USDT. "
                    "Consider reducing position size to improve execution quality."
                )

                patterns.append(AtomicPattern(
                    pattern_id="position_high_spread",
                    user_id=user_id,
                    position_key=position_key,
                    message=message,
                    description=description,
                    severity=severity,
                    unique=True,
                    ttl_minutes=60 * 24,
                    details={
                        "position_key": position_key,
                        "symbol": symbol,
                        "venue": venue,
                        "spread": spread,
                        "threshold": self.LIQUIDITY_THRESHOLD,
                        "position_size": position_size,
                        "market_depth": depth
                    }
                ))

            if depth < self.MIN_LIQUIDITY_DEPTH:
                violation_ratio = self.MIN_LIQUIDITY_DEPTH / depth if depth > 0 else float('inf')
                severity = self.calculate_dynamic_severity(violation_ratio, max_violation=4.0)

                message = f"Position {position_key} in low liquidity market (depth: {depth:,.0f} USDT)"
                description = (
                    f"Market depth ({depth:,.0f} USDT) is below minimum threshold ({self.MIN_LIQUIDITY_DEPTH:,.0f} USDT). "
                    f"Position size: {position_size:.2f} USDT, Depth ratio: {depth_ratio:.2%}. "
                    "Consider reducing position size to minimize market impact."
                )

                patterns.append(AtomicPattern(
                    pattern_id="position_low_liquidity",
                    user_id=user_id,
                    position_key=position_key,
                    message=message,
                    description=description,
                    severity=severity,
                    unique=True,
                    details={
                        "position_key": position_key,
                        "symbol": symbol,
                        "venue": venue,
                        "market_depth": depth,
                        "threshold": self.MIN_LIQUIDITY_DEPTH,
                        "position_size": position_size,
                        "depth_ratio": depth_ratio
                    }
                ))

        return patterns

    async def check_coin_hype(self, user_id: int, position_histories: Dict[str, List[Position]]) -> List[AtomicPattern]:
        """
        Check if positions are in coins experiencing hype based on Google Trends data.
        
        Args:
            user_id: User ID being evaluated
            position_histories: Dictionary mapping position keys to lists of Position objects
            
        Returns:
            List of patterns for positions in hyped coins
        """
        patterns = []

        for position_key, history in position_histories.items():
            if not history:
                continue

            current_position = history[0]
            if current_position.qty == 0:
                continue

            try:
                venue, symbol = position_key.split('_', 1)
            except ValueError:
                logger.warning(f"Invalid position key format: {position_key}")
                continue

            hype_metrics = await self.trends_service.analyze_hype_metrics(symbol)
            if not hype_metrics:
                continue

            hype_score = hype_metrics.get('hype_score', 0.0)
            current_interest = hype_metrics.get('current_interest', 0.0)
            historical_avg = hype_metrics.get('historical_avg', 0.0)
            interest_change = hype_metrics.get('interest_change', 0.0)
            deviation_from_avg = hype_metrics.get('deviation_from_avg', 0.0)
            is_above_average = hype_metrics.get('is_above_average', False)

            if hype_score >= self.HYPE_THRESHOLD:
                severity = min(1.0, hype_score)

                if hype_score >= 0.9:
                    message = f"Extreme hype detected for {symbol} (score: {hype_score:.2f})"
                    description = (
                        f"Current interest ({current_interest:.1f}) is significantly above historical average "
                        f"({historical_avg:.1f}). Interest has changed by {interest_change:+.1f}% in the last period. "
                        "Consider reviewing position size and risk management strategy."
                    )
                elif hype_score >= 0.8:
                    message = f"High hype level for {symbol} (score: {hype_score:.2f})"
                    description = (
                        f"Current interest ({current_interest:.1f}) is well above historical average "
                        f"({historical_avg:.1f}). Interest has changed by {interest_change:+.1f}% in the last period. "
                        "Monitor position closely for potential volatility."
                    )
                elif hype_score >= 0.7:
                    message = f"Significant hype for {symbol} (score: {hype_score:.2f})"
                    description = (
                        f"Current interest ({current_interest:.1f}) is above historical average "
                        f"({historical_avg:.1f}). Interest has changed by {interest_change:+.1f}% in the last period. "
                        "Be aware of increased market attention."
                    )
                else:
                    message = f"Moderate hype for {symbol} (score: {hype_score:.2f})"
                    description = (
                        f"Current interest ({current_interest:.1f}) is slightly above historical average "
                        f"({historical_avg:.1f}). Interest has changed by {interest_change:+.1f}% in the last period."
                    )

                patterns.append(AtomicPattern(
                    pattern_id="position_coin_hype",
                    user_id=user_id,
                    position_key=position_key,
                    message=message,
                    description=description,
                    severity=severity,
                    unique=True,
                    ttl_minutes=60 * 24,
                    category_weights={
                        RiskCategory.FOMO: 0.8,
                    },
                    details={
                        "position_key": position_key,
                        "symbol": symbol,
                        "venue": venue,
                        "hype_score": hype_score,
                        "current_interest": current_interest,
                        "historical_avg": historical_avg,
                        "interest_change": interest_change,
                        "deviation_from_avg": deviation_from_avg,
                        "is_above_average": is_above_average,
                        "position_size": current_position.usdt_amt,
                        "unrealized_pnl": current_position.unrealized_pnl
                    }
                ))

        return patterns
