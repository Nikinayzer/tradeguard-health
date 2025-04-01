"""
Batch Risk Analyzer

Performs deeper analysis of user behavior across complete historical data,
identifying patterns that require a holistic view of user activity.
"""

from datetime import datetime
from typing import Dict, List, Any
from collections import Counter, defaultdict

from src.utils.datetime_utils import DateTimeUtils
from src.utils.log_util import get_logger

logger = get_logger()


class UserBatchAnalyzer:
    """
    Performs batch analysis of user behavior across all historic data,
    looking for patterns that require a holistic view.
    """
    
    def __init__(self):
        """Initialize the batch analyzer"""
        self.evaluator_id = "user_batch_analyzer"
        self.description = "Analyzes user behavior across complete historical data"
        
    def analyze_user(self, user_id: int, job_history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Analyze a user's complete job history for risk patterns.
        
        Args:
            user_id: User ID
            job_history: User's complete job history
            
        Returns:
            List of evidence dictionaries
        """
        evidence = []
        
        # Skip if no history
        if not job_history or len(job_history) < 5:
            return evidence
            
        # Pattern 1: Time-based patterns (unusual hours, weekends, etc.)
        evidence.extend(self._analyze_time_patterns(user_id, job_history))
        
        # Pattern 2: Job similarity and repetition
        evidence.extend(self._analyze_job_similarity(user_id, job_history))
        
        # Pattern 3: Trading strategy patterns (could be added in the future)
        
        return evidence
        
    def _analyze_time_patterns(self, user_id: int, job_history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Analyze time patterns across all historical data.
        
        Looks for patterns like:
        - High concentration of trades during unusual hours
        - Weekend trading patterns
        - Other temporal anomalies
        """
        evidence = []
        
        # Extract timestamps
        timestamps = []
        for job in job_history:
            job_timestamp = job.get("timestamp")
            if job_timestamp:
                dt = DateTimeUtils.parse_timestamp(job_timestamp)
                if dt:
                    timestamps.append(dt)
        
        if not timestamps:
            return evidence
            
        # Group by hour
        hours = [ts.hour for ts in timestamps]
        hour_counts = Counter(hours)
        
        # Check for concentration in unusual hours
        total_jobs = len(hours)
        unusual_hours = set(range(0, 5))  # Midnight to 5am
        
        unusual_hour_count = sum(hour_counts.get(hour, 0) for hour in unusual_hours)
        unusual_hour_ratio = unusual_hour_count / total_jobs if total_jobs > 0 else 0
        
        # If more than 30% of jobs are in unusual hours
        if unusual_hour_ratio > 0.3 and unusual_hour_count >= 3:
            confidence = min(0.9, 0.5 + unusual_hour_ratio * 0.5)
            
            evidence.append({
                "category_id": "time_pattern",
                "confidence": confidence,
                "evaluator_id": self.evaluator_id,
                "data": {
                    "unusual_hour_count": unusual_hour_count,
                    "total_jobs": total_jobs,
                    "unusual_hour_ratio": unusual_hour_ratio,
                    "reason": "High concentration of trades during unusual hours",
                    "hour_distribution": {str(h): c for h, c in hour_counts.items()}
                },
                "source": "batch"
            })
        
        # Analyze day of week patterns
        days = [ts.weekday() for ts in timestamps]
        day_counts = Counter(days)
        
        weekend_days = {5, 6}  # Saturday and Sunday
        weekend_count = sum(day_counts.get(day, 0) for day in weekend_days)
        weekend_ratio = weekend_count / total_jobs if total_jobs > 0 else 0
        
        # If more than 50% of jobs are on weekends
        if weekend_ratio > 0.5 and weekend_count >= 3:
            confidence = min(0.8, 0.4 + weekend_ratio * 0.4)
            
            evidence.append({
                "category_id": "time_pattern",
                "confidence": confidence,
                "evaluator_id": self.evaluator_id,
                "data": {
                    "weekend_count": weekend_count,
                    "total_jobs": total_jobs,
                    "weekend_ratio": weekend_ratio,
                    "reason": "High concentration of trades during weekends",
                    "day_distribution": {str(d): c for d, c in day_counts.items()}
                },
                "source": "batch"
            })
            
        return evidence
        
    def _analyze_job_similarity(self, user_id: int, job_history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Analyze similarity between jobs to detect patterns.
        
        Looks for:
        - Identical trade amounts across job history
        - Repeated parameters or strategies
        - Other behavioral signatures
        """
        evidence = []
        
        # Group by job type
        job_types = defaultdict(list)
        for job in job_history:
            job_type = job.get("job_type", "unknown")
            job_types[job_type].append(job)
            
        # Analyze each job type with enough samples
        for job_type, jobs in job_types.items():
            if len(jobs) < 3:
                continue
                
            # Extract amounts
            amounts = [
                job.get("parameters", {}).get("amount", 0) 
                for job in jobs
            ]
            amounts = [amt for amt in amounts if amt > 0]
            
            if len(amounts) < 3:
                continue
                
            # Check for identical amounts (suspicious pattern)
            amount_counts = Counter(amounts)
            most_common_amount, count = amount_counts.most_common(1)[0]
            
            if count >= 3 and count / len(amounts) > 0.5:
                confidence = min(0.85, 0.5 + (count / len(amounts)) * 0.5)
                
                evidence.append({
                    "category_id": "sunk_cost",
                    "confidence": confidence,
                    "evaluator_id": self.evaluator_id,
                    "data": {
                        "job_type": job_type,
                        "repeated_amount": most_common_amount,
                        "repeat_count": count,
                        "total_jobs": len(amounts),
                        "repeat_ratio": count / len(amounts),
                        "reason": "Pattern of repeated identical trade amounts"
                    },
                    "source": "batch"
                })
                
        return evidence
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert analyzer to dictionary for serialization"""
        return {
            "evaluator_id": self.evaluator_id,
            "description": self.description
        } 