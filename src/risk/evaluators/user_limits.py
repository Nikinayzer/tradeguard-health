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

from src.models import Job, AtomicPattern, RiskCategory
from src.models.position_models import PositionUpdateType, Position
from src.risk.evaluators.base import BaseRiskEvaluator, RiskDataProvider
from src.utils.datetime_utils import DateTimeUtils
from src.utils.log_util import get_logger
from src.models.user_models import UserLimits
from src.config.config import Config
from src.state.state_manager import StateManager

logger = get_logger()


def _get_user_limits(user_id: int) -> Optional[UserLimits]:
    """
    Get the user's trading limits, with caching to avoid excessive API calls.

    Args:
        user_id: User ID

    Returns:
        UserLimits object (either from API or defaults)
    """
    try:
        url = f"{Config.BFF_BASE_URL}{Config.API_USER_LIMITS_PATH.format(user_id=user_id)}"
        logger.debug(f"Fetching limits for user {user_id} from {url}")

        response = requests.get(url=url, timeout=10.0)

        if response.status_code == 200:
            data = response.json()
            logger.info(f"Successfully fetched user limits: {data}")

            mapped_data = {
                "id": data.get("id", user_id),
                "userId": data.get("userId", user_id),
                "maxSingleJobLimit": data.get("maxSingleJobLimit", 10000),
                "maxDailyTradingLimit": data.get("maxDailyTradingLimit", 50000),
                "maxPortfolioRisk": data.get("maxPortfolioRisk", 0.1),
                "maxConcurrentOrders": data.get("maxConcurrentOrders", 5),
                "maxDailyTrades": data.get("maxDailyTrades", 20),
                "tradingCooldown": data.get("tradingCooldown", 5),
                "dailyLossLimit": data.get("dailyLossLimit", 1000),
                "maxConsecutiveLosses": data.get("maxConsecutiveLosses", 3),
                "maxDailyBalanceChange": data.get("maxDailyBalanceChange", 0.2),
                #"volatilityLimit": data.get("volatilityLimit", 0.05),
                #"liquidityThreshold": data.get("liquidityThreshold", 1000)
            }

            logger.info(f"Mapped data for UserLimits model: {mapped_data}")

            limits = UserLimits(**mapped_data)

            logger.info(
                f"User {user_id} limits: daily trades={limits.max_daily_trades}, position size={limits.max_position_size}, concurrent jobs={limits.max_concurrent_jobs}")
            return limits

    except Exception as e:
        logger.error(f"Error fetching user limits for user {user_id}: {str(e)}")
        return None


class UserLimitsEvaluator(BaseRiskEvaluator):
    """Evaluates if a user is exceeding their self-defined trading limits"""

    def __init__(self, state_manager: StateManager):
        """Initialize the user limits evaluator"""
        super().__init__(
            evaluator_id="user_limits_evaluator",
            description="Checks job against user-defined trading limits",
            state_manager=state_manager
        )

    def evaluate(self, user_id: int) -> List[AtomicPattern]:
        """
        Evaluate user limits violations.
        
        Args:
            user_id: User ID
            
        Returns:
            List of patterns dictionaries
        """
        try:
            logger.info(f"[UserLimitsEvaluator] Starting evaluation for user {user_id}")
            job_history = self.state_manager.job_storage.get_user_jobs(user_id, 24)
            if not job_history:
                logger.info(f"[UserLimitsEvaluator] No job history found for user {user_id}")
                return []
            last_key = next(reversed(job_history))
            job = job_history[last_key]
            logger.info(f"[UserLimitsEvaluator] Latest job timestamp: {job.timestamp}, tzinfo: {job.timestamp.tzinfo}")

            position_histories = self.state_manager.position_storage.get_user_position_histories(
                user_id=user_id,
                hours=24
            )

            patterns = []

            user_limits = _get_user_limits(user_id)
            logger.info(f"[UserLimitsEvaluator] Got user limits: {user_limits}")

            if not user_limits:
                logger.error(f"Could not get valid user limits for user {user_id}, skipping evaluation")
                return patterns

            logger.info("[UserLimitsEvaluator] Running checks:")

            try:
                logger.debug("[UserLimitsEvaluator] Running single job limit check...")
                if pattern := (self._check_single_job_limit(job=job, limits=user_limits)):
                    patterns.append(pattern)
            except Exception as e:
                logger.error(f"[UserLimitsEvaluator] Error in single job limit check: {str(e)}")

            try:
                logger.debug("[UserLimitsEvaluator] Running daily trades limit check...")
                if pattern := (self._check_daily_trades_limit(job_history=job_history, limits=user_limits)):
                    patterns.append(pattern)
            except Exception as e:
                logger.error(f"[UserLimitsEvaluator] Error in daily trades limit check: {str(e)}")

            try:
                logger.debug("[UserLimitsEvaluator] Running daily volume limit check...")
                if pattern := self._check_daily_volume_limit(position_histories=position_histories, limits=user_limits):
                    patterns.append(pattern)
            except Exception as e:
                logger.error(f"[UserLimitsEvaluator] Error in daily volume limit check: {str(e)}")

            try:
                logger.debug("[UserLimitsEvaluator] Running trade cooldown check...")
                if pattern := (self._check_trade_cooldown(job_history=job_history, limits=user_limits)):
                    patterns.append(pattern)
            except Exception as e:
                logger.error(f"[UserLimitsEvaluator] Error in trade cooldown check: {str(e)}")

            try:
                logger.debug("[UserLimitsEvaluator] Running concurrent jobs check...")
                if pattern := self._check_concurrent_jobs(job_history=job_history, limits=user_limits):
                    patterns.append(pattern)
            except Exception as e:
                logger.error(f"[UserLimitsEvaluator] Error in concurrent jobs check: {str(e)}")

            try:
                logger.debug("[UserLimitsEvaluator] Running force job check...")
                if pattern := self._check_force(job=job):
                    patterns.append(pattern)
            except Exception as e:
                logger.error(f"[UserLimitsEvaluator] Error in force job check: {str(e)}")

            logger.info(f"[UserLimitsEvaluator] Evaluation complete. Found {len(patterns)} patterns")
            return patterns
        except Exception as e:
            logger.error(f"[UserLimitsEvaluator] Error in evaluate: {str(e)}")
            raise

    def _check_single_job_limit(self, job: Job, limits: UserLimits) -> Optional[AtomicPattern]:
        """Check if job amount exceeds single job limit"""
        logger.info(f"[UserLimitsEvaluator] Checking single job limits for job {job.job_id}")
        if not limits or not hasattr(limits, 'max_position_size') or not limits.max_position_size:
            logger.warning("Missing or invalid position size limit, skipping check")
            return None

        if job.amount > limits.max_position_size > 0 and job.is_dca_job:
            violation_rate = job.amount / limits.max_position_size
            severity = self.calculate_dynamic_severity(violation_rate)

            pattern = AtomicPattern(
                pattern_id="limit_single_job_amount",
                job_id=[job.job_id],
                message=f"Maximum amount for job {job.id} exceeded",
                severity=severity,
                unique=True,
                ttl_minutes=60 * 24,
                details={
                    "actual": job.amount,
                    "limit": limits.max_position_size,
                    "ratio": violation_rate,
                }
            )
            logger.info(
                f"[UserLimitsEvaluator] Created single job pattern with start_time={pattern.start_time}, tzinfo={pattern.start_time.tzinfo}")
            return pattern
        return None

    def _check_daily_trades_limit(self, job_history: Dict[int, Job],
                                  limits: UserLimits) -> Optional[AtomicPattern]:
        """Check if daily trades count exceeds limit"""
        logger.info("[UserLimitsEvaluator] Checking daily trades limits for user job history")
        if not limits or not hasattr(limits, 'max_daily_trades') or not limits.max_daily_trades:
            logger.warning("Missing or invalid daily trades limit, skipping check")
            return None

        trade_count = len(job_history)
        logger.info(f"[UserLimitsEvaluator] Found {trade_count} trades")

        if trade_count >= limits.max_daily_trades > 0:
            violation_rate = trade_count / limits.max_daily_trades
            severity = self.calculate_dynamic_severity(violation_rate)

            pattern = AtomicPattern(
                pattern_id="limit_daily_trades_count",
                message="Daily limit of jobs was violated",
                severity=severity,
                unique=True,
                ttl_minutes=60 * 24,
                details={
                    "actual": trade_count,
                    "limit": limits.max_daily_trades,
                    "ratio": violation_rate,
                }
            )
            logger.info(
                f"[UserLimitsEvaluator] Created daily trades pattern with start_time={pattern.start_time}, tzinfo={pattern.start_time.tzinfo}")
            return pattern
        return None

    def _check_daily_volume_limit(self, position_histories: Dict[str, List[Position]],
                                  limits: UserLimits) -> Optional[AtomicPattern]:
        """
        Checks if daily volume exceeds limit based on position changes.

        Args:
            limits: User limits containing max_daily_volume

        Returns:
            AtomicPattern if limit is exceeded, None otherwise
        """
        logger.info("Checking daily volume limits for user position histories")
        if not limits or not hasattr(limits, 'max_daily_volume') or not limits.max_daily_volume:
            logger.warning("Missing or invalid daily volume limit, skipping check")
            return None

        total_volume = 0
        last_positions = {}  # symbol -> Position

        for position_key, history in position_histories.items():
            try:
                venue, symbol = position_key.split('_', 1)
                for position in history:
                    if position.update_type == PositionUpdateType.SNAPSHOT:
                        last_positions[symbol] = position
                        continue

                    if position.update_type in [PositionUpdateType.INCREASED, PositionUpdateType.DECREASED,
                                                PositionUpdateType.CLOSED]:
                        last_position = last_positions.get(symbol)
                        if last_position:
                            volume_change = abs(position.usdt_amt - last_position.usdt_amt)
                            total_volume += volume_change

                        last_positions[symbol] = position

            except ValueError as e:
                logger.error(f"Error processing position history for {position_key}: {e}")
                continue

        if total_volume >= limits.max_daily_volume > 0:
            violation_ratio = total_volume / limits.max_daily_volume
            severity = self.calculate_dynamic_severity(violation_ratio)

            return AtomicPattern(
                pattern_id="limit_daily_volume",
                message=f"Daily volume limit exceeded",
                coins=list(last_positions.keys()),
                severity=severity,
                category_weights={RiskCategory.OVERCONFIDENCE: 1.0},
                ttl_minutes=60 * 24,
                unique=True,
                details={
                    "actual_volume": total_volume,
                    "limit": limits.max_daily_volume,
                    "violation_ratio": violation_ratio,
                }
            )

        return None

    def _check_trade_cooldown(self, job_history: Dict[int, Job],
                              limits: UserLimits) -> Optional[AtomicPattern]:
        """
        Check if trade cooldown period is violated between the last two jobs.
        
        Args:
            job_history: Dictionary of jobs (job_id -> Job)
            limits: User limits configuration

        Returns:
            Detected cooldown violation pattern or null
        """
        logger.info("[UserLimitsEvaluator] Checking trade cooldown limits for user job history")
        if not limits or not hasattr(limits, 'min_trade_interval_minutes') or not limits.min_trade_interval_minutes:
            logger.warning("Missing or invalid trade cooldown limit, skipping check")
            return None

        jobs = list(job_history.values())
        if len(jobs) < 2:
            logger.info("[UserLimitsEvaluator] Less than 2 jobs found, skipping cooldown check")
            return None

        jobs.sort(key=lambda j: j.timestamp)
        current_job = jobs[-1]
        previous_job = jobs[-2]

        # Calculate time difference in minutes
        time_diff = current_job.timestamp - previous_job.timestamp
        minutes_diff = time_diff.total_seconds() / 60

        cooldown_minutes = limits.min_trade_interval_minutes
        if minutes_diff < cooldown_minutes:
            minutes_early = cooldown_minutes - minutes_diff
            violation_ratio = minutes_early / cooldown_minutes
            severity = self.calculate_dynamic_severity(violation_ratio, inverted=True)

            pattern = AtomicPattern(
                pattern_id="limit_cooldown",
                job_id=[current_job.id],
                message=f"Cooldown between strategies {current_job.id} and {previous_job.id} was violated",
                severity=severity,
                unique=True,
                ttl_minutes=60 * 24,
                details={
                    "actual_interval_minutes": round(minutes_diff, 2),
                    "required_cooldown_minutes": cooldown_minutes,
                    "violation_minutes": round(minutes_early, 2),
                    "violation_ratio": round(violation_ratio, 3),
                    "severity_score": round(severity, 3),
                }
            )
            logger.info(
                f"[UserLimitsEvaluator] Created cooldown pattern with start_time={pattern.start_time}, tzinfo={pattern.start_time.tzinfo}")
            return pattern

        return None

    def _check_concurrent_jobs(self, job_history: Dict[int, Job],
                               limits: UserLimits) -> Optional[AtomicPattern]:
        """
        Check if concurrent jobs limit is exceeded at any point in the job history.
        This method analyzes active jobs to identify periods when too many jobs
        were running simultaneously.

        Args:
            job_history: Dictionary of jobs (job_id -> Job)
            limits: User limits configuration

        Returns:
            Detected concurrent jobs violation pattern or null
        """
        logger.info("[UserLimitsEvaluator] Checking concurrent jobs limits for user job history")
        if not limits or not hasattr(limits, 'max_concurrent_jobs') or not limits.max_concurrent_jobs:
            logger.warning("Missing or invalid concurrent jobs limit, skipping check")
            return None

        open_jobs = []
        for history_job in job_history.values():
            if history_job.is_active:
                open_jobs.append(history_job)

        open_jobs_count = len(open_jobs) - 1
        max_concurrent = limits.max_concurrent_jobs

        if open_jobs_count > max_concurrent:
            violation_ratio = open_jobs_count / max_concurrent
            severity = self.calculate_dynamic_severity(violation_ratio)
            open_job_ids = [j.job_id for j in open_jobs]

            pattern = AtomicPattern(
                pattern_id="limit_concurrent_jobs",
                job_id=open_job_ids,
                unique=True,
                message=f"Concurrent jobs limit exceeded",
                severity=severity,
                ttl_minutes=60 * 24,
                start_time=open_jobs[0].timestamp,
                end_time=open_jobs[-1].timestamp,
                details={
                    "actual": open_jobs_count,
                    "limit": max_concurrent,
                    "ratio": violation_ratio,
                }
            )
            logger.info(
                f"[UserLimitsEvaluator] Created concurrent jobs pattern with start_time={pattern.start_time}, tzinfo={pattern.start_time.tzinfo}")
            return pattern

        return None

    def _check_force(self, job: Job) -> Optional[AtomicPattern]:
        if job.discount_pct == 0:
            return AtomicPattern(
                pattern_id="limit_force_job",
                job_id=[job.id],
                unique=True,
                message=f"Job {job.id} has force parameter.",
                severity=1.0,
                ttl_minutes=60 * 24,
            )
        return None
