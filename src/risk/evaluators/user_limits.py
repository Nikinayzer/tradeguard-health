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
from datetime import datetime, timedelta, timezone
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

        # If we couldn't get valid user limits, return empty patterns
        if not user_limits:
            logger.error(f"Could not get valid user limits for user {user_id}, skipping evaluation")
            return patterns

        patterns.extend(self._check_single_job_limit(job, user_limits))
        patterns.extend(self._check_daily_trades_limit(job, job_history, user_limits))
        patterns.extend(self._check_daily_volume_limit(job, job_history, user_limits))
        patterns.extend(self._check_trade_cooldown(job, job_history, user_limits))
        patterns.extend(self._check_concurrent_jobs(job, job_history, user_limits))
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

    # todo refactor to work with all jobs
    def _check_single_job_limit(self, job: Job, limits: UserLimits) -> List[Pattern]:
        """Check if job amount exceeds single job limit"""
        patterns = []

        # Safety check: verify limits is valid and has required values
        if not limits or not hasattr(limits, 'max_position_size') or not limits.max_position_size:
            logger.warning("Missing or invalid position size limit, skipping check")
            return patterns

        if job.amount > limits.max_position_size > 0:
            violation_rate = job.amount / limits.max_position_size

            confidence = self.calculate_dynamic_confidence(
                violation_rate,
                base=0.3,
                scaling=0.2,
            )

            patterns.append(Pattern(
                pattern_id="single_job_amount_limit",
                job_id=[job.job_id],
                message="Single job amount exceeded",
                confidence=confidence,
                category_weights={RiskCategory.FOMO: 0.6,
                                  RiskCategory.SUNK_COST: 0.2,
                                  },
                details={
                    "actual": job.amount,
                    "limit": limits.max_position_size,
                    "ratio": violation_rate,
                }
            ))

        return patterns

    def _check_daily_trades_limit(self, job: Job, job_history: Dict[int, Job],
                                  limits: UserLimits) -> List[Pattern]:
        """Check if daily trades count exceeds limit"""
        patterns = []

        # Safety check: verify limits is valid and has required values
        if not limits or not hasattr(limits, 'max_daily_trades') or not limits.max_daily_trades:
            logger.warning("Missing or invalid daily trades limit, skipping check")
            return patterns

        trade_count = len(job_history)

        if trade_count >= limits.max_daily_trades > 0:
            violation_rate = trade_count / limits.max_daily_trades

            confidence = self.calculate_dynamic_confidence(
                violation_rate,
                base=0.3,
                scaling=0.2,
            )

            patterns.append(Pattern(
                pattern_id="daily_trade_limit",
                job_id=[job.job_id],
                message="Daily trades limit exceeded",
                confidence=confidence,
                category_weights={RiskCategory.OVERTRADING: 0.9,
                                  RiskCategory.FOMO: 0.1},
                details={
                    "actual": trade_count,
                    "limit": limits.max_daily_trades,
                    "ratio": violation_rate,
                }
            ))

        return patterns

    def _check_daily_volume_limit(self, job: Job, job_history: Dict[int, Job],
                                  limits: UserLimits) -> List[Pattern]:
        """Check if daily volume exceeds limit"""
        patterns = []

        # Safety check: verify limits is valid and has required values
        if not limits or not hasattr(limits, 'max_daily_volume') or not limits.max_daily_volume:
            logger.warning("Missing or invalid daily volume limit, skipping check")
            return patterns

        total_volume = 0
        for history_job in job_history.values():
            total_volume += history_job.amount

        if total_volume >= limits.max_daily_volume > 0:
            violation_rate = total_volume / limits.max_daily_volume
            confidence = self.calculate_dynamic_confidence(
                violation_rate,
                base=0.3,
                scaling=0.2,
            )

            patterns.append(Pattern(
                pattern_id="daily_volume_exceeded",
                job_id=[job.job_id],
                message="Daily volume limit exceeded",
                confidence=confidence,
                category_weights={RiskCategory.OVERTRADING: 0.6,
                                  RiskCategory.FOMO: 0.2,
                                  RiskCategory.SUNK_COST: 0.2},
                details={
                    "actual": total_volume,
                    "limit": limits.max_daily_volume,
                    "ratio": violation_rate
                }
            ))

        return patterns

    def _check_trade_cooldown(self, job: Job, job_history: Dict[int, Job],
                              limits: UserLimits) -> List[Pattern]:
        """Check if trade cooldown period is violated"""
        patterns = []

        # Safety check: verify limits is valid and has required values
        if not limits or not hasattr(limits, 'min_trade_interval_minutes') or not limits.min_trade_interval_minutes:
            logger.warning("Missing or invalid trade cooldown limit, skipping check")
            return patterns

        # Current job's timestamp as datetime
        current_time = job.created_at

        most_recent_time = None

        for history_job in job_history.values():
            if history_job.job_id == job.job_id:
                continue

            # Get timestamp as datetime directly
            job_dt = history_job.created_at

            if most_recent_time is None or job_dt > most_recent_time:
                most_recent_time = job_dt

        # If no valid previous timestamp found, return early
        if not most_recent_time:
            return patterns

        time_diff = current_time - most_recent_time
        minutes_diff = time_diff.total_seconds() / 60
        cooldown_minutes = limits.min_trade_interval_minutes

        if minutes_diff < cooldown_minutes:
            violation_ratio = (cooldown_minutes - minutes_diff) / cooldown_minutes
            confidence = self.calculate_dynamic_confidence(
                violation_ratio,
                base=0.5,
                scaling=0.2,
            )

            patterns.append(Pattern(
                pattern_id="cooldown_limit",
                job_id=[job.job_id],
                message="Trade cooldown limit violated",
                confidence=confidence,
                category_weights={
                    RiskCategory.OVERTRADING: 0.5,
                    RiskCategory.FOMO: 0.5
                },
                details={
                    "actual": minutes_diff,
                    "limit": cooldown_minutes,
                    "ratio": violation_ratio,
                }
            ))

        return patterns

    # todo later if needed - rewrite using timestamp searching retrospectively
    def _check_concurrent_jobs(self, job: Job, job_history: Dict[int, Job],
                               limits: UserLimits) -> List[Pattern]:
        """Check if concurrent jobs limit is exceeded"""
        patterns = []

        if not limits or not hasattr(limits, 'max_concurrent_jobs') or not limits.max_concurrent_jobs:
            logger.warning("Missing or invalid concurrent jobs limit, skipping check")
            return patterns

        # Count open jobs excluding the current one
        open_jobs = []
        for history_job in job_history.values():
            if history_job.is_active:
                open_jobs.append(history_job)

        open_jobs_count = len(open_jobs) - 1
        max_concurrent = limits.max_concurrent_jobs

        # Check if limit exceeded
        if open_jobs_count > max_concurrent:
            # Calculate confidence based on severity of violation
            violation_ratio = open_jobs_count / max_concurrent

            # Use our dynamic confidence calculation for consistency
            confidence = self.calculate_dynamic_confidence(
                violation_ratio,
                base=0.4,
                scaling=0.25,
            )

            open_job_ids = [j.job_id for j in open_jobs]

            # Create the pattern
            patterns.append(Pattern(
                pattern_id="concurrent_jobs_limit",
                job_id=open_job_ids,
                message=f"Concurrent jobs limit exceeded",
                confidence=confidence,
                category_weights={
                    RiskCategory.OVERTRADING: 0.7,
                    RiskCategory.FOMO: 0.3
                },
                details={
                    "actual": open_jobs_count,
                    "limit": max_concurrent,
                    "ratio": violation_ratio,
                }
            ))

        return patterns

    def _check_allow_job(self, job: Job, job_history: Dict[int, Job], limits: UserLimits) -> List[Pattern]:
        patterns = []
        if limits.allowDcaForce != 0 and job.discount_pct == 0:
            patterns.append(Pattern(
                pattern_id="cooldown_limit",
                job_id=[job.job_id],
                message="Trade cooldown limit violated",
                confidence=0.2,
                category_weights={
                    RiskCategory.OVERTRADING: 0.5,
                    RiskCategory.FOMO: 0.5
                },
                details={
                    "actual": minutes_diff,
                    "limit": cooldown_minutes,
                    "ratio": violation_ratio,
                }
            ))
        return patterns

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
