"""
Base Risk Evaluator

Provides the foundation class for all risk evaluators.
"""

from typing import Dict, List, Any, Optional

from src.models import Job
from src.utils.log_util import get_logger

logger = get_logger()


class BaseRiskEvaluator:
    """Base class for all risk evaluators"""
    
    def __init__(self, evaluator_id: str, description: str):
        """
        Initialize the base evaluator.
        
        Args:
            evaluator_id: Unique identifier for this evaluator
            description: Description of what this evaluator analyzes
        """
        self.evaluator_id = evaluator_id
        self.description = description
        
    def evaluate(self, user_id: int, job: Job, job_history: Dict[int, Job]) -> List[Dict[str, Any]]:
        """
        Evaluate risk for a job.
        
        Args:
            user_id: User ID
            job: Current job as a Job object
            job_history: User's job history as a dictionary mapping job_id to Job objects
            
        Returns:
            List of evidence dictionaries, each containing:
                - category_id: Risk category ID
                - confidence: Confidence level (0-1)
                - data: Additional evidence data
        """
        # Abstract method to be implemented by subclasses
        raise NotImplementedError("Subclasses must implement evaluate()")
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert evaluator to dictionary for serialization"""
        return {
            "evaluator_id": self.evaluator_id,
            "description": self.description
        }
