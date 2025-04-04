"""
User Limits Evaluator

Checks job data against user-defined limits, flagging violations as risk patterns.
This covers limits on:
- Single job size
- Daily trade count
- Daily volume
- Cooldown between trades
- Concurrent jobs
"""
import math

import requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from src.models import Job
from src.risk.evaluators.base import BaseRiskEvaluator
from src.utils.datetime_utils import DateTimeUtils
from src.utils.log_util import get_logger
from src.models.user_models import UserLimits
from src.models.risk_models import RiskCategory, Pattern
from src.config.config import Config

logger = get_logger()


class UserLimitsEvaluator(BaseRiskEvaluator):
    """Evaluates if a user is exceeding their self-defined trading limits"""

    # Define explicit category mappings for different types of limits
    # This makes the business domain logic explicit about which limit checks
    # map to which risk categories
    RISK_CATEGORY_MAPPINGS = {
        # Position sizing related checks
        "single_job_limit": RiskCategory.POSITION_SIZE,
        "max_portfolio_risk": RiskCategory.PORTFOLIO_EXPOSURE,

        # Overtrading related checks
        "daily_trades_limit": RiskCategory.OVERTRADING,
        "daily_volume_limit": RiskCategory.OVERTRADING,

        # Time pattern related checks
        "trade_cooldown": RiskCategory.TIME_PATTERN,

        # Execution related checks
        "concurrent_jobs": RiskCategory.EXECUTION,

        # Sunk cost related checks
        "max_consecutive_losses": RiskCategory.SUNK_COST,
        "daily_loss_limit": RiskCategory.SUNK_COST,

        # FOMO related checks
        "liquidity_threshold": RiskCategory.FOMO,
        "volatility_limit": RiskCategory.FOMO,
    }

    def __init__(self):
        """Initialize the user limits evaluator"""
        super().__init__(
            evaluator_id="user_limits_evaluator",
            description="Checks job against user-defined trading limits"
        )
        # todo doesnt belong here, just testing
        self.user_limits_cache: Dict[int, UserLimits] = {}
        self.cache_ttl_seconds = 1
        self.cache_timestamps: Dict[int, datetime] = {}

    def evaluate(self, user_id: int, job_history: Dict[int, Job]) -> List[Pattern]:
        """
        Evaluate user limits violations.
        
        Args:
            user_id: User ID
            job_history: User's job history as a dictionary mapping job_id to Job objects
            
        Returns:
            List of patterns dictionaries
        """
        last_key = next(reversed(job_history))  # HOPE THIS WORKS BECAUSE ELSE I WILL KMS
        job = job_history[last_key]

        logger.info(f"UserLimitsEvaluator: Evaluating job {job.job_id} for user {user_id}")

        patterns = []

        user_limits = self._get_user_limits(user_id)

        patterns.extend(self._check_single_job_limit(job, user_limits))
        # patterns.extend(self._check_daily_trades_limit(job, job_history, user_limits))
        # patterns.extend(self._check_daily_volume_limit(job, job_history, user_limits))
        # patterns.extend(self._check_trade_cooldown(job, job_history, user_limits))
        # patterns.extend(self._check_concurrent_jobs(job, job_history, user_limits))
        #
        # patterns.extend(self._check_consecutive_losses(job, job_history, user_limits))
        # patterns.extend(self._check_liquidity_threshold(job, user_limits))

        return patterns

    def _get_user_limits(self, user_id: int) -> Optional[UserLimits]:
        """
        Get the user's trading limits, with caching to avoid excessive API calls.
        
        Args:
            user_id: User ID
            
        Returns:
            UserLimits object (either from API or defaults)
        """
        # Check if we have cached limits that aren't expired
        now = datetime.now()
        if user_id in self.user_limits_cache:
            cache_time = self.cache_timestamps.get(user_id)
            if cache_time and (now - cache_time).total_seconds() < self.cache_ttl_seconds:
                logger.info(f"Using cached limits for user {user_id}")
                logger.info(self.user_limits_cache[user_id])
                return self.user_limits_cache[user_id]

        try:
            url = f"{Config.API_BASE_URL}{Config.API_USER_LIMITS_PATH.format(user_id=user_id)}"
            logger.debug(f"Fetching limits for user {user_id} from {url}")

            response = requests.get(url=url, timeout=10.0)

            if response.status_code == 200:
                data = response.json()
                logger.info(f"Successfully fetched user limits: {data}")

                mapped_data = {
                    "id": data.get("id", user_id),
                    "userId": 493077349684740097,
                    "maxSingleJobLimit": data.get("maxSingleJobLimit", 10000),
                    "maxDailyTradingLimit": data.get("maxDailyTradingLimit", 50000),
                    "maxPortfolioRisk": data.get("maxPortfolioRisk", 0.1),
                    "maxConcurrentOrders": data.get("maxConcurrentOrders", 5),
                    "maxDailyTrades": data.get("maxDailyTrades", 20),
                    "tradingCooldown": data.get("tradingCooldown", 5),
                    "allowDcaForce": data.get("allowDcaForce", True),
                    "allowLiqForce": data.get("allowLiqForce", True),
                    "dailyLossLimit": data.get("dailyLossLimit", 1000),
                    "maxConsecutiveLosses": data.get("maxConsecutiveLosses", 3),
                    "maxDailyBalanceChange": data.get("maxDailyBalanceChange", 0.2),
                    "volatilityLimit": data.get("volatilityLimit", 0.05),
                    "liquidityThreshold": data.get("liquidityThreshold", 1000)
                }

                logger.info(f"Mapped data for UserLimits model: {mapped_data}")

                limits = UserLimits(**mapped_data)

                self.user_limits_cache[user_id] = limits
                self.cache_timestamps[user_id] = now

                logger.info(
                    f"User {user_id} limits: daily trades={limits.max_daily_trades}, position size={limits.max_position_size}, concurrent jobs={limits.max_concurrent_jobs}")
                return limits

        except Exception as e:
            logger.error(f"Error fetching user limits for user {user_id}: {str(e)}")
            return None

    def _check_single_job_limit(self, job: Job, limits: UserLimits) -> List[Pattern]:
        """Check if job amount exceeds single job limit"""
        patterns = []

        amount = job.amount
        if amount > limits.max_position_size:
            BASE_CONFIDENCE = 0.6
            SCALING_FACTOR = 0.1
            MAX_CONFIDENCE = 1.0

            # 10 / 5 = 2
            violation_ratio = amount / limits.max_position_size

            confidence = BASE_CONFIDENCE + SCALING_FACTOR * math.log1p(violation_ratio * 10)
            confidence = min(MAX_CONFIDENCE, confidence)

            patterns.append(Pattern(
                pattern_id="single_job_limit",
                job_id=[job.job_id],
                message="Single job limit exceeded",
                confidence=confidence,
                category_weights={RiskCategory.FOMO: 0.6,
                                  RiskCategory.SUNK_COST: 0.3},
                details={
                    "amount": amount,
                    "limit": limits.max_position_size,
                    "ratio": amount / limits.max_position_size,
                }
            ))

        return patterns

    def _check_daily_trades_limit(self, job: Job, job_history: Dict[int, Job],
                                  limits: UserLimits) -> List[Dict[str, Any]]:
        """Check if daily trades count exceeds limit"""
        evidence = []

        # Get trades from today
        today = datetime.now().date()
        todays_trades = []

        # More efficient loop through dictionary values
        for history_job in job_history.values():
            job_timestamp = history_job.timestamp
            if job_timestamp:
                dt = DateTimeUtils.parse_timestamp(job_timestamp)
                if dt and dt.date() == today:
                    todays_trades.append(history_job)

        # Include current job
        trade_count = len(todays_trades) + 1

        # Check if exceeded limit
        if trade_count >= limits.max_daily_trades:
            confidence = max(0.2, 0.5 + (trade_count / limits.max_daily_trades) * 0.1)

            # Use the explicit category mapping
            category = self.RISK_CATEGORY_MAPPINGS["daily_trades_limit"].value

            evidence.append({
                "category_id": category,
                "confidence": confidence,
                "evaluator_id": self.evaluator_id,
                "data": {
                    "job_id": job.job_id,
                    "trade_count": trade_count,
                    "limit": limits.max_daily_trades,
                    "ratio": trade_count / limits.max_daily_trades,
                    "reason": "Daily trade limit exceeded"
                }
            })

        return evidence

    def _check_daily_volume_limit(self, job: Job, job_history: Dict[int, Job],
                                  limits: UserLimits) -> List[Dict[str, Any]]:
        """Check if daily volume exceeds limit"""
        evidence = []
        # Get trades from today
        today = datetime.now().date()
        todays_volume = 0

        # More efficient calculation of daily volume
        for history_job in job_history.values():
            job_timestamp = history_job.timestamp
            if job_timestamp:
                dt = DateTimeUtils.parse_timestamp(job_timestamp)
                if dt and dt.date() == today:
                    todays_volume += history_job.amount

        # Add current job amount
        total_volume = todays_volume + job.amount

        # Check if exceeded limit
        if total_volume >= limits.max_daily_volume:
            confidence = min(0.85, 0.5 + (total_volume / limits.max_daily_volume) * 0.1)

            # Use the explicit category mapping
            category = self.RISK_CATEGORY_MAPPINGS["daily_volume_limit"].value

            evidence.append({
                "category_id": category,
                "confidence": confidence,
                "evaluator_id": self.evaluator_id,
                "data": {
                    "job_id": job.job_id,
                    "daily_volume": total_volume,
                    "limit": limits.max_daily_volume,
                    "ratio": total_volume / limits.max_daily_volume,
                    "reason": "Daily volume limit exceeded"
                }
            })

        return evidence

    def _check_trade_cooldown(self, job: Job, job_history: Dict[int, Job],
                              limits: UserLimits) -> List[Dict[str, Any]]:
        """Check if trade cooldown period is violated"""
        evidence = []

        # Get current job time
        current_timestamp = job.timestamp
        if not current_timestamp:
            current_time = datetime.now()
        else:
            current_time = DateTimeUtils.parse_timestamp(current_timestamp) or datetime.now()

        # Find most recent trade
        most_recent_trade = None
        most_recent_time = None

        for history_job in job_history.values():
            job_timestamp = history_job.timestamp
            dt = None
            if not job_timestamp:
                #continue
                dt = DateTimeUtils.parse_timestamp(job_timestamp)
            if not dt:
                continue

            if most_recent_time is None or dt > most_recent_time:
                most_recent_time = dt
                most_recent_trade = history_job

        # If no previous trade, or trade is first of the day, no cooldown needed
        if not most_recent_time:
            return evidence

        # Check if cooldown period has passed
        time_diff = current_time - most_recent_time
        minutes_diff = time_diff.total_seconds() / 60
        cooldown_minutes = limits.min_trade_interval_minutes

        if minutes_diff < cooldown_minutes:
            # Calculate how many minutes remain in the cooldown period
            cooldown_remaining_minutes = cooldown_minutes - minutes_diff

            # Higher confidence for more severe violations
            # 0.6 for just under cooldown, up to 0.9 for immediate repeat trades
            violation_ratio = (cooldown_minutes - minutes_diff) / cooldown_minutes
            confidence = 0.6 + violation_ratio * 0.3

            # Use the explicit category mapping
            category = self.RISK_CATEGORY_MAPPINGS["trade_cooldown"].value

            evidence.append({
                "category_id": category,
                "confidence": confidence,
                "evaluator_id": self.evaluator_id,
                "data": {
                    "job_id": job.job_id,
                    "minutes_since_last_trade": round(minutes_diff, 2),
                    "cooldown_minutes": cooldown_minutes,
                    "cooldown_remaining_minutes": round(cooldown_remaining_minutes, 2),
                    "last_trade_id": most_recent_trade.job_id if most_recent_trade else None,
                    "reason": "Trading cooldown period violated"
                }
            })

        return evidence

    def _check_concurrent_jobs(self, job: Job, job_history: Dict[int, Job],
                               limits: UserLimits) -> List[Dict[str, Any]]:
        """Check if concurrent jobs limit is exceeded"""
        evidence = []

        # Count open jobs excluding the current one
        open_jobs = []
        for history_job in job_history.values():
            if history_job.status in ["Created", "In Progress"] and history_job.job_id != job.job_id:
                open_jobs.append(history_job)

        # Add current job to count
        open_jobs_count = len(open_jobs) + 1
        max_concurrent = limits.max_concurrent_jobs

        # Check if limit exceeded
        if open_jobs_count > max_concurrent:
            # Calculate confidence based on severity of violation
            ratio = open_jobs_count / max_concurrent
            confidence = min(0.95, 0.6 + (ratio - 1) * 0.1)

            # Get IDs of open jobs for reference
            open_job_ids = [j.job_id for j in open_jobs]

            # Use the explicit category mapping
            category = self.RISK_CATEGORY_MAPPINGS["concurrent_jobs"].value

            evidence.append({
                "category_id": category,
                "confidence": confidence,
                "evaluator_id": self.evaluator_id,
                "data": {
                    "job_id": job.job_id,
                    "open_jobs_count": open_jobs_count,
                    "limit": max_concurrent,
                    "ratio": ratio,
                    "open_job_ids": open_job_ids,
                    "reason": "Concurrent jobs limit exceeded"
                }
            })

        return evidence

    def _check_consecutive_losses(self, job: Job, job_history: Dict[int, Job],
                                  limits: UserLimits) -> List[Dict[str, Any]]:
        """Check if user has exceeded consecutive loss limit (SUNK_COST category)"""
        evidence = []

        # Only check for new jobs
        if job.status != "Created":
            return evidence

        # Count recent consecutive losses
        # This is just a skeleton implementation
        consecutive_losses = 0  # In a real implementation, calculate from job_history
        max_consecutive_losses = limits.maxConsecutiveLosses

        if consecutive_losses >= max_consecutive_losses:
            confidence = min(0.9, 0.5 + (consecutive_losses / max_consecutive_losses) * 0.1)

            # Use the explicit category mapping - this is SUNK_COST, different from other limits
            category = self.RISK_CATEGORY_MAPPINGS["max_consecutive_losses"].value

            evidence.append({
                "category_id": category,
                "confidence": confidence,
                "evaluator_id": self.evaluator_id,
                "data": {
                    "job_id": job.job_id,
                    "consecutive_losses": consecutive_losses,
                    "limit": max_consecutive_losses,
                    "ratio": consecutive_losses / max_consecutive_losses,
                    "reason": "Consecutive losses limit exceeded"
                }
            })

        return evidence

    def _check_liquidity_threshold(self, job: Job, limits: UserLimits) -> List[Dict[str, Any]]:
        """Check if job exceeds liquidity threshold (FOMO category)"""
        evidence = []

        # Only check for new jobs 
        if job.status != "Created":
            return evidence

        # This is a skeleton implementation - in a real system you'd get market data
        market_liquidity = 1000000  # Example value, in a real impl get from market data
        job_amount = job.amount
        liquidity_threshold = limits.liquidityThreshold

        # If job amount is high relative to market liquidity
        if job_amount > liquidity_threshold:
            liquidity_ratio = job_amount / market_liquidity
            confidence = min(0.85, 0.4 + liquidity_ratio * 0.5)

            # Use the explicit category mapping - this is FOMO, different from others
            category = self.RISK_CATEGORY_MAPPINGS["liquidity_threshold"].value

            evidence.append({
                "category_id": category,
                "confidence": confidence,
                "evaluator_id": self.evaluator_id,
                "data": {
                    "job_id": job.job_id,
                    "job_amount": job_amount,
                    "market_liquidity": market_liquidity,
                    "liquidity_threshold": liquidity_threshold,
                    "liquidity_ratio": liquidity_ratio,
                    "reason": "Job size exceeds liquidity threshold"
                }
            })

        return evidence
