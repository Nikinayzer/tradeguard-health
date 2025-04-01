"""
Overtrading Risk Evaluator

Identifies users who trade too frequently or with excessive volume.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any

from src.risk.evaluators.base import BaseRiskEvaluator
from src.utils.datetime_utils import DateTimeUtils
from src.utils.log_util import get_logger

logger = get_logger()


class OverTradingEvaluator(BaseRiskEvaluator):
    """Evaluates if a user is overtrading based on frequency and volume"""
    
    def __init__(self):
        """Initialize the overtrading evaluator"""
        super().__init__(
            evaluator_id="overtrading_evaluator",
            description="Analyzes job frequency and volume to identify overtrading patterns"
        )
        
        # Configuration parameters - will be moved to config
        self.frequency_threshold = 10  # Jobs per day
        self.volume_threshold = 5000   # Large job amount
        self.lookback_hours = 24       # Lookback period for frequency analysis
        
    def evaluate(self, user_id: int, job_data: Dict[str, Any], job_history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Evaluate overtrading risk based on job frequency and volume.
        
        Args:
            user_id: User ID
            job_data: Current job data
            job_history: User's job history
            
        Returns:
            List of evidence dictionaries
        """
        # Initialize results
        evidence = []
        
        # Get job amount
        amount = job_data.get("amount", 0)
        
        # Check for large volume jobs (pattern 1)
        if amount and amount > self.volume_threshold:
            # Calculate confidence based on how much it exceeds threshold
            volume_ratio = amount / self.volume_threshold
            confidence = min(0.9, 0.5 + (volume_ratio - 1) * 0.1)
            
            evidence.append({
                "category_id": "overtrading",
                "confidence": confidence,
                "data": {
                    "job_id": job_data.get("job_id"),
                    "amount": amount,
                    "threshold": self.volume_threshold,
                    "reason": "Large volume job",
                    "volume_ratio": volume_ratio
                }
            })
        
        # Check frequency of jobs (pattern 2)
        if job_history:
            # Get current time
            now = datetime.now()
            
            # Calculate lookback time
            lookback_time = now - timedelta(hours=self.lookback_hours)
            
            # Count jobs in lookback period
            recent_jobs = []
            for job in job_history:
                job_timestamp = job.get("timestamp")
                if job_timestamp:
                    dt = DateTimeUtils.parse_timestamp(job_timestamp)
                    if dt and dt > lookback_time:
                        recent_jobs.append(job)
            
            # Add current job to count
            recent_job_count = len(recent_jobs) + 1
            
            # Calculate jobs per day
            jobs_per_day = (recent_job_count / self.lookback_hours) * 24
            
            # If frequency exceeds threshold, add evidence
            if jobs_per_day > self.frequency_threshold:
                # Calculate confidence based on how much it exceeds threshold
                frequency_ratio = jobs_per_day / self.frequency_threshold
                confidence = min(0.9, 0.4 + (frequency_ratio - 1) * 0.2)
                
                evidence.append({
                    "category_id": "overtrading",
                    "confidence": confidence,
                    "data": {
                        "job_id": job_data.get("job_id"),
                        "job_count": recent_job_count,
                        "time_period_hours": self.lookback_hours,
                        "jobs_per_day": jobs_per_day,
                        "threshold": self.frequency_threshold,
                        "reason": "High trading frequency",
                        "frequency_ratio": frequency_ratio
                    }
                })
        
        return evidence 