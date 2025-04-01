"""
Time Pattern Risk Evaluator

Identifies users exhibiting unusual time patterns in their trading behavior,
such as trading during unusual hours or with suspiciously consistent timing.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any

from src.risk.evaluators.base import BaseRiskEvaluator
from src.utils.datetime_utils import DateTimeUtils
from src.utils.log_util import get_logger

logger = get_logger()


class TimePatternEvaluator(BaseRiskEvaluator):
    """Evaluates unusual time patterns in trading behavior"""
    
    def __init__(self):
        """Initialize the time pattern evaluator"""
        super().__init__(
            evaluator_id="time_pattern_evaluator",
            description="Analyzes unusual timing patterns in trading behavior"
        )
        
        # Configuration parameters
        self.unusual_hours = set(range(0, 5))  # Unusual trading hours (midnight to 5am)
        self.max_gap_minutes = 2             # Max gap between jobs to consider consecutive
        
    def evaluate(self, user_id: int, job_data: Dict[str, Any], job_history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Evaluate unusual time patterns in trading.
        
        Args:
            user_id: User ID
            job_data: Current job data
            job_history: User's job history
            
        Returns:
            List of evidence dictionaries
        """
        evidence = []
        
        # Pattern 1: Trading during unusual hours
        job_timestamp = job_data.get("timestamp")
        if job_timestamp:
            dt = DateTimeUtils.parse_timestamp(job_timestamp)
            if dt:
                evidence.extend(self._check_unusual_hours(dt))
        
        # Pattern 2: Suspiciously consistent timing
        if job_history and len(job_history) >= 3:
            evidence.extend(self._check_consistent_timing(job_data, job_history))
        
        return evidence
    
    def _check_unusual_hours(self, dt: datetime) -> List[Dict[str, Any]]:
        """
        Check if time is during unusual trading hours.
        
        Args:
            dt: The datetime to check
            
        Returns:
            List of evidence dictionaries
        """
        evidence = []
        
        # Check time of job
        hour = dt.hour
        
        # Check if job is created during unusual hours
        if hour in self.unusual_hours:
            confidence = 0.5  # Base confidence for unusual hours
            
            evidence.append({
                "category_id": "time_pattern",
                "confidence": confidence,
                "data": {
                    "hour": hour,
                    "timestamp": dt.isoformat(),
                    "unusual_hours": list(self.unusual_hours),
                    "reason": "Trading during unusual hours",
                }
            })
            
        return evidence
    
    def _check_consistent_timing(self, job_data: Dict[str, Any], 
                             job_history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Check for suspiciously consistent timing between trades, which might
        indicate automated trading.
        """
        evidence = []
        
        # Extract timestamps and parse them
        job_timestamps = []
        for job in job_history:
            timestamp = job.get("timestamp")
            if timestamp:
                dt = DateTimeUtils.parse_timestamp(timestamp)
                if dt:
                    job_timestamps.append((job.get("job_id"), dt))
        
        # Add current job
        current_timestamp = job_data.get("timestamp")
        if current_timestamp:
            dt = DateTimeUtils.parse_timestamp(current_timestamp)
            if dt:
                job_timestamps.append((job_data.get("job_id"), dt))
        
        # Check if we have enough history
        if len(job_timestamps) >= 3:
            # Sort by timestamp
            job_timestamps.sort(key=lambda x: x[1])
            
            # Calculate time gaps
            gaps = []
            for i in range(1, len(job_timestamps)):
                prev_time = job_timestamps[i-1][1]
                curr_time = job_timestamps[i][1]
                gap_seconds = (curr_time - prev_time).total_seconds()
                gaps.append(gap_seconds)
            
            # Calculate consistency of gaps
            if gaps and len(gaps) >= 2:
                avg_gap = sum(gaps) / len(gaps)
                
                # Calculate variance and standard deviation
                variance = sum((gap - avg_gap) ** 2 for gap in gaps) / len(gaps)
                stddev = variance ** 0.5
                
                # If standard deviation is small relative to average, highly consistent timing
                if avg_gap > 0 and stddev / avg_gap < 0.2:  # Less than 20% variation
                    confidence = min(0.8, 0.4 + 0.4 * (1 - stddev / avg_gap))
                    
                    evidence.append({
                        "category_id": "time_pattern",
                        "confidence": confidence,
                        "data": {
                            "job_id": job_data.get("job_id"),
                            "avg_gap_seconds": avg_gap,
                            "stddev_seconds": stddev,
                            "variation_ratio": stddev / avg_gap,
                            "job_count": len(job_timestamps),
                            "reason": "Highly consistent timing between trades",
                        }
                    })
        
        return evidence 