"""
Sunk Cost Fallacy Risk Evaluator

Identifies users exhibiting sunk cost fallacy behavior, where they continue to
invest in a particular strategy despite evidence it may not be working.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any
from collections import Counter

from src.risk.evaluators.base import BaseRiskEvaluator
from src.utils.datetime_utils import DateTimeUtils
from src.utils.log_util import get_logger

logger = get_logger()


class SunkCostEvaluator(BaseRiskEvaluator):
    """Evaluates if a user is exhibiting sunk cost fallacy behavior"""
    
    def __init__(self):
        """Initialize the sunk cost evaluator"""
        super().__init__(
            evaluator_id="sunk_cost_evaluator",
            description="Analyzes behavior patterns to identify potential sunk cost fallacy"
        )
        
        # Configuration parameters
        self.lookback_hours = 48       # Lookback period for analysis
        
    def evaluate(self, user_id: int, job_data: Dict[str, Any], job_history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Evaluate sunk cost risk based on persistent similar trades after initial ones.
        
        Args:
            user_id: User ID
            job_data: Current job data
            job_history: User's job history
            
        Returns:
            List of evidence dictionaries
        """
        evidence = []
        
        # We need history to evaluate sunk cost
        if not job_history:
            return evidence
            
        # Get job type
        job_type = job_data.get("name", "unknown")
        
        # Pattern 1: Repeated similar trades over time
        evidence.extend(self._check_repetitive_patterns(job_data, job_history, job_type))
        
        # Pattern 2: Increasing frequency of same type (could add in the future)
        
        return evidence
        
    def _check_repetitive_patterns(self, job_data: Dict[str, Any], 
                                job_history: List[Dict[str, Any]],
                                job_type: str) -> List[Dict[str, Any]]:
        """
        Check for repetitive trading patterns that may indicate sunk cost.
        """
        evidence = []
        
        # Get lookback period
        now = datetime.now()
        lookback_time = now - timedelta(hours=self.lookback_hours)
        
        # Filter history by time
        recent_history = []
        for job in job_history:
            job_timestamp = job.get("timestamp")
            if job_timestamp:
                dt = DateTimeUtils.parse_timestamp(job_timestamp)
                if dt and dt > lookback_time:
                    recent_history.append(job)
        
        # Count jobs by type
        job_type_counts = Counter([job.get("job_type", "unknown") for job in recent_history])
        
        # If current job type appears frequently, potential sunk cost
        if job_type in job_type_counts and job_type_counts[job_type] >= 3:
            # Calculate confidence based on repetition
            repetition_count = job_type_counts[job_type]
            confidence = min(0.85, 0.3 + repetition_count * 0.1)
            
            evidence.append({
                "category_id": "sunk_cost",
                "confidence": confidence,
                "data": {
                    "job_id": job_data.get("job_id"),
                    "job_type": job_type,
                    "repetition_count": repetition_count,
                    "period_hours": self.lookback_hours,
                    "reason": "Repeated similar trades over time",
                }
            })
            
        return evidence 