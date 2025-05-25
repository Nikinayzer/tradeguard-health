"""
Trading Behavior Evaluator

Evaluates trading behavior patterns.
"""
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional, Set, Tuple
from collections import defaultdict

from src.models import Job, AtomicPattern, RiskCategory
from src.risk.evaluators.base import BaseRiskEvaluator
from src.state.state_manager import StateManager
from src.utils.log_util import get_logger

logger = get_logger()


class TradingBehaviorEvaluator(BaseRiskEvaluator):
    """Evaluates trading behavior patterns."""

    def __init__(self, state_manager: StateManager):
        """Initialize the evaluator."""
        super().__init__(
            evaluator_id="trading_behavior",
            description="Evaluates trading behavior patterns",
            state_manager=state_manager
        )

    def evaluate(self, user_id: int) -> List[AtomicPattern]:
        """
        Evaluate position behaviors across the entire job history.
        
        Args:
            user_id: User ID
            
        Returns:
            List of pattern objects
        """
        job_history = self.state_manager.get_user_jobs(user_id)
        if not job_history:
            return []

        logger.info(f"PositionBehaviorEvaluator: Evaluating job history for user {user_id} ({len(job_history)} jobs)")

        patterns = []

        patterns.extend(self._check_position_size_acceleration(job_history))

        return patterns

    # todo decide if limited timeframe is needed
    def _check_position_size_acceleration(self, job_history: Dict[int, Job]) -> List[AtomicPattern]:
        """
        Detect rapid acceleration in position sizes over time.
        
        This looks for a trend of increasingly larger positions, which may indicate
        escalating risk-taking behavior.
        """
        patterns = []

        # Need at least 3 jobs to detect acceleration
        if len(job_history) < 3:
            return patterns

        # Group jobs by coin
        coin_jobs = defaultdict(list)

        valid_jobs = []
        for job in job_history.values():
            if job.is_dca_job and job.coins and job.amount > 0:
                valid_jobs.append(job)

        # Sort jobs by timestamp
        valid_jobs.sort(key=lambda j: j.created_at)

        for job in valid_jobs:
            coin = job.coins[0]  # Use first coin as the identifier
            coin_jobs[coin].append(job)

        # Look for acceleration patterns in each coin
        for coin, jobs in coin_jobs.items():
            # Need at least 3 jobs to detect acceleration
            if len(jobs) < 3:
                continue

            # Look for sequences of 3+ jobs with increasing sizes
            for i in range(2, len(jobs)):
                job3 = jobs[i]
                job2 = jobs[i - 1]
                job1 = jobs[i - 2]

                ratio1 = job2.amount / job1.amount if job1.amount > 0 else 0
                ratio2 = job3.amount / job2.amount if job2.amount > 0 else 0

                if 1.1 < ratio1 < ratio2 and ratio2 >= 1.3:
                    total_growth = job3.amount / job1.amount

                    acceleration_factor = ratio2 / ratio1
                    severity = self.calculate_dynamic_severity(
                        acceleration_factor,
                    )

                    patterns.append(AtomicPattern(
                        pattern_id="position_acceleration",
                        job_id=[job1.id, job2.id, job3.id],
                        message=f"Accelerating position sizes detected in {coin}",
                        severity=severity,
                        details={
                            "coin": coin,
                            "job_amounts": [job1.amount, job2.amount, job3.amount],
                            "growth_ratios": [ratio1, ratio2],
                            "acceleration_factor": acceleration_factor,
                            "total_growth": total_growth,
                            "job_ids": [job1.job_id, job2.job_id, job3.job_id],
                            "start_time": job1.created_at,
                            "end_time": job3.created_at,
                            "days_span": (job3.created_at - job1.created_at).total_seconds() / (24 * 3600)
                        }
                    ))

        return patterns
