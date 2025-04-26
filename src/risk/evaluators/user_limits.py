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
        patterns.extend(self._check_daily_volume_limit(job_history, user_limits))
        patterns.extend(self._check_trade_cooldown(job_history, user_limits))
        patterns.extend(self._check_concurrent_jobs(job_history, user_limits))
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
                pattern_id="limit_single_job_amount",
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
                pattern_id="limit_daily_trades_count",
                message="Daily limit of jobs was violated",
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

    def _check_daily_volume_limit(self, job_history: Dict[int, Job],
                                  limits: UserLimits) -> List[Pattern]:
        """Check if daily volume exceeds limit"""
        patterns = []

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
                pattern_id="limit_daily_volume",
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
        """
        Check if trade cooldown period is violated across all jobs in history.
        This method analyzes the entire job history to find any instances where
        jobs were created too close together, violating the cooldown period.

        Args:
            job_history: Dictionary of jobs (job_id -> Job)
            limits: User limits configuration

        Returns:
            List of detected cooldown violation patterns
        """
        patterns = []

        if not limits or not hasattr(limits, 'min_trade_interval_minutes') or not limits.min_trade_interval_minutes:
            logger.warning("Missing or invalid trade cooldown limit, skipping check")
            return patterns

        cooldown_minutes = limits.min_trade_interval_minutes
        jobs_list = list(job_history.values())
        jobs_list.sort(key=lambda j: j.created_at)

        for i in range(1, len(jobs_list)):
            current_job = jobs_list[i]
            previous_job = jobs_list[i - 1]

            time_diff = current_job.created_at - previous_job.created_at
            minutes_diff = time_diff.total_seconds() / 60

            if minutes_diff < cooldown_minutes:
                violation_ratio = (cooldown_minutes - minutes_diff) / cooldown_minutes
                confidence = self.calculate_dynamic_confidence(
                    violation_ratio,
                    base=0.3,
                    scaling=0.2,
                )
                patterns.append(Pattern(
                    pattern_id="limit_cooldown",
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
    def _check_concurrent_jobs(self, job_history: Dict[int, Job],
                               limits: UserLimits) -> List[Pattern]:
        """
        Check if concurrent jobs limit is exceeded at any point in the job history.
        This method analyzes active jobs to identify periods when too many jobs
        were running simultaneously.

        Args:
            job_history: Dictionary of jobs (job_id -> Job)
            limits: User limits configuration

        Returns:
            List of detected concurrent jobs violation patterns
        """
        patterns = []

        if not limits or not hasattr(limits, 'max_concurrent_jobs') or not limits.max_concurrent_jobs:
            logger.warning("Missing or invalid concurrent jobs limit, skipping check")
            return patterns

        open_jobs = []
        for history_job in job_history.values():
            if history_job.is_active:
                open_jobs.append(history_job)

        open_jobs_count = len(open_jobs) - 1
        max_concurrent = limits.max_concurrent_jobs

        if open_jobs_count > max_concurrent:
            violation_ratio = open_jobs_count / max_concurrent

            confidence = self.calculate_dynamic_confidence(
                violation_ratio,
                base=0.4,
                scaling=0.25,
            )

            open_job_ids = [j.job_id for j in open_jobs]

            patterns.append(Pattern(
                pattern_id="limit_concurrent_jobs",
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

    def _check_allow_force(self, job_history: Dict[int, Job], limits: UserLimits) -> List[Pattern]:
        patterns = []
        if not limits or not hasattr(limits, 'allow_dca_force') or limits.allowDcaForce:
            logger.warning("Missing or invalid concurrent jobs limit, skipping check")
            return patterns

        violated_jobs = []
        for history_job in job_history.values():
            if history_job.discount_pct == 0:
                violated_jobs.append(history_job)
            patterns.append(Pattern(
                pattern_id="limit_force_job",
                job_id=[history_job.job_id],
                message="Found a job violating force limit",
                confidence=0.2,
                category_weights={
                    RiskCategory.OVERTRADING: 0.5,
                    RiskCategory.FOMO: 0.5
                }
            ))
        return patterns
