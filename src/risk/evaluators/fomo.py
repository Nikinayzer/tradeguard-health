"""
FOMO Risk Evaluator

Identifies users exhibiting "Fear of Missing Out" behavior patterns through
rapid successive trades and escalating volumes.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any

from src.risk.evaluators.base import BaseRiskEvaluator
from src.utils.datetime_utils import DateTimeUtils
from src.utils.log_util import get_logger

logger = get_logger()


class FOMOEvaluator(BaseRiskEvaluator):
    """Evaluates if a user is exhibiting Fear of Missing Out behavior"""
    
    def __init__(self):
        """Initialize the FOMO evaluator"""
        super().__init__(
            evaluator_id="fomo_evaluator",
            description="Analyzes behavior patterns to identify potential FOMO"
        )
        
        # Configuration parameters
        self.short_interval_minutes = 5       # Short interval for rapid successive trades
        self.increasing_steps_threshold = 3    # Number of increasing steps to detect escalation
        
    def evaluate(self, user_id: int, job_data: Dict[str, Any], job_history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Evaluate FOMO risk based on rapid successive trades and escalating volumes.
        
        Args:
            user_id: User ID
            job_data: Current job data
            job_history: User's job history
            
        Returns:
            List of evidence dictionaries
        """
        evidence = []
        
        # Check for FOMO indicators
        if not job_history:
            return evidence
            
        # Get current time and job info
        now = datetime.now()
        current_amount = job_data.get("amount", 0)
        
        # Pattern 1: Rapid successive trades
        evidence.extend(self._check_rapid_trades(job_data, job_history, now))
        
        # Pattern 2: Escalating volumes
        if current_amount > 0:
            evidence.extend(self._check_escalating_volumes(job_data, job_history, current_amount))
                
        return evidence
    
    def _check_rapid_trades(self, job_data: Dict[str, Any], job_history: List[Dict[str, Any]], 
                         current_time: datetime) -> List[Dict[str, Any]]:
        """
        Check for rapid successive trades (impulsive behavior).
        """
        evidence = []
        
        # Calculate short interval time
        short_interval = current_time - timedelta(minutes=self.short_interval_minutes)
        
        # Find recent jobs within the short interval
        recent_jobs = []
        for job in job_history:
            job_timestamp = job.get("timestamp")
            if job_timestamp:
                dt = DateTimeUtils.parse_timestamp(job_timestamp)
                if dt and dt > short_interval:
                    recent_jobs.append(job)
        
        # If there are very recent jobs, might indicate impulsive behavior
        if recent_jobs:
            # More recent jobs indicates higher confidence
            job_count = len(recent_jobs)
            confidence = min(0.8, 0.4 + job_count * 0.1)
            
            evidence.append({
                "category_id": "fomo",
                "confidence": confidence,
                "data": {
                    "job_id": job_data.get("job_id"),
                    "recent_job_count": job_count,
                    "interval_minutes": self.short_interval_minutes,
                    "reason": "Rapid successive trades",
                }
            })
            
        return evidence
    
    def _check_escalating_volumes(self, job_data: Dict[str, Any], job_history: List[Dict[str, Any]],
                               current_amount: float) -> List[Dict[str, Any]]:
        """
        Check for escalating trade volumes.
        """
        evidence = []
        
        # Extract timestamps and sort history by timestamp
        job_timestamps = []
        for job in job_history:
            job_timestamp = job.get("timestamp")
            if job_timestamp:
                dt = DateTimeUtils.parse_timestamp(job_timestamp)
                if dt:
                    amount = job.get("parameters", {}).get("amount", 0)
                    job_timestamps.append((job, dt, amount))
        
        # Sort by timestamp
        sorted_jobs = sorted(job_timestamps, key=lambda x: x[1])
        
        # Extract amounts in chronological order
        amounts = [a for _, _, a in sorted_jobs if a > 0]
        
        # Pattern 2a: Current trade significantly larger than historical max
        if amounts and current_amount > max(amounts) * 1.5:
            confidence = min(0.85, 0.5 + 0.1 * (current_amount / max(amounts)))
            
            evidence.append({
                "category_id": "fomo",
                "confidence": confidence,
                "data": {
                    "job_id": job_data.get("job_id"),
                    "current_amount": current_amount,
                    "previous_max": max(amounts),
                    "increase_ratio": current_amount / max(amounts),
                    "reason": "Significant volume increase",
                }
            })
        
        # Pattern 2b: Consecutive increasing amounts
        if len(amounts) >= 2:
            increasing_steps = 0
            for i in range(1, len(amounts)):
                if amounts[i] > amounts[i-1] * 1.2:  # 20% increase
                    increasing_steps += 1
                else:
                    increasing_steps = 0
                    
            # Check if current job continues the pattern
            if increasing_steps > 0 and current_amount > amounts[-1] * 1.2:
                increasing_steps += 1
                
            if increasing_steps >= self.increasing_steps_threshold:
                confidence = min(0.9, 0.5 + increasing_steps * 0.1)
                
                evidence.append({
                    "category_id": "fomo",
                    "confidence": confidence,
                    "data": {
                        "job_id": job_data.get("job_id"),
                        "increasing_steps": increasing_steps,
                        "threshold": self.increasing_steps_threshold,
                        "reason": "Pattern of escalating trade volumes",
                    }
                })
        
        return evidence 