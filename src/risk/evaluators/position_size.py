"""
Position Size Risk Evaluator

Identifies users taking on excessive position sizes relative to 
their typical behavior or absolute thresholds.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any

from src.risk.evaluators.base import BaseRiskEvaluator
from src.utils.datetime_utils import DateTimeUtils
from src.utils.log_util import get_logger

logger = get_logger()


class PositionSizeEvaluator(BaseRiskEvaluator):
    """Evaluates if a user is taking on excessive position sizes"""
    
    def __init__(self):
        """Initialize the position size evaluator"""
        super().__init__(
            evaluator_id="position_size_evaluator",
            description="Analyzes job amounts relative to user's typical behavior"
        )
        
        # Configuration parameters
        self.absolute_threshold = 10000     # Absolute size threshold
        self.relative_threshold = 3.0      # Multiple of user's average
        self.lookback_days = 30            # Days to look back for average
        
    def evaluate(self, user_id: int, job_data: Dict[str, Any], job_history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Evaluate position size risk.
        
        Args:
            user_id: User ID
            job_data: Current job data
            job_history: User's job history
            
        Returns:
            List of evidence dictionaries
        """
        evidence = []
        
        # Get amount from job data
        amount = job_data.get("amount", 0)
        if amount <= 0:
            return evidence  # No risk if no amount or negative
        
        # Pattern 1: Exceeds absolute threshold
        evidence.extend(self._check_absolute_threshold(job_data, amount))
        
        # Pattern 2: Exceeds relative threshold based on user history
        if job_history:
            evidence.extend(self._check_relative_threshold(job_data, amount, job_history))
        
        return evidence
    
    def _check_absolute_threshold(self, job_data: Dict[str, Any], amount: float) -> List[Dict[str, Any]]:
        """
        Check if amount exceeds the absolute threshold.
        """
        evidence = []
        
        if amount > self.absolute_threshold:
            # Calculate confidence based on how much it exceeds threshold
            ratio = amount / self.absolute_threshold
            confidence = min(0.9, 0.5 + (ratio - 1) * 0.1)
            
            evidence.append({
                "category_id": "position_size",
                "confidence": confidence,
                "data": {
                    "job_id": job_data.get("job_id"),
                    "amount": amount,
                    "absolute_threshold": self.absolute_threshold,
                    "ratio": ratio,
                    "reason": "Exceeds absolute position size threshold",
                }
            })
            
        return evidence
    
    def _check_relative_threshold(self, job_data: Dict[str, Any], amount: float, 
                              job_history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Check if amount exceeds the user's typical trading amount.
        """
        evidence = []
        
        # Get lookback period
        now = datetime.now()
        lookback_time = now - timedelta(days=self.lookback_days)
        
        # Get historical amounts from jobs within lookback period
        historical_amounts = []
        for job in job_history:
            job_timestamp = job.get("timestamp")
            if job_timestamp:
                dt = DateTimeUtils.parse_timestamp(job_timestamp)
                if dt and dt > lookback_time:
                    job_amount = job.get("parameters", {}).get("amount", 0)
                    if job_amount > 0:
                        historical_amounts.append(job_amount)
        
        if historical_amounts:
            avg_amount = sum(historical_amounts) / len(historical_amounts)
            
            if avg_amount > 0 and amount > avg_amount * self.relative_threshold:
                ratio = amount / avg_amount
                confidence = min(0.9, 0.4 + (ratio / self.relative_threshold) * 0.2)
                
                evidence.append({
                    "category_id": "position_size",
                    "confidence": confidence,
                    "data": {
                        "job_id": job_data.get("job_id"),
                        "amount": amount,
                        "avg_historical_amount": avg_amount,
                        "relative_threshold": self.relative_threshold,
                        "ratio": ratio,
                        "reason": "Significantly exceeds user's average position size",
                    }
                })
                
        return evidence 