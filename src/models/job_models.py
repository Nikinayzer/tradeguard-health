from datetime import datetime
from typing import List, Optional, Dict, Any, ClassVar
from pydantic import BaseModel, Field, field_validator

from src.utils import log_util

logger = log_util.get_logger()


class JobEvent(BaseModel):
    """Base model for all job events received from Kafka."""
    job_id: int
    user_id: Optional[int] = None
    event_type: str
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "JobEvent":
        """Create a JobEvent from a dictionary."""
        return cls(**data)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the model to a dictionary."""
        return self.model_dump()


class Job(BaseModel):
    """Model representing a trading job with its full state."""
    job_id: int
    user_id: int
    event_type: str
    name: Optional[str] = ""
    coins: List[str] = []
    side: Optional[str] = ""
    discount_pct: float = 0.0
    amount: float = 0.0
    steps_total: int = 0
    duration_minutes: float = 0.0
    timestamp: str
    status: Optional[str] = ""
    completed_steps: Optional[int] = 0
    orders: Optional[List[Dict[str, Any]]] = []
    
    # Constants for job types
    DCA_JOB_NAMES: ClassVar[List[str]] = ["dca", "Dca", "DCA"]
    LIQ_JOB_NAMES: ClassVar[List[str]] = ["liq", "Liq", "LIQ"]
    TERMINAL_STATUSES: ClassVar[List[str]] = ["Finished", "Stopped"]

    @property
    def id(self) -> int:
        """Alias for job_id to maintain compatibility with existing code"""
        return self.job_id

    @property
    def created_at(self) -> str:
        """Alias for timestamp"""
        return self.timestamp

    @property
    def updated_at(self) -> str:
        """Alias for timestamp"""
        return self.timestamp

    @property
    def job_status(self) -> str:
        """Return status if available, otherwise map event_type to status for compatibility"""
        if self.status:
            return self.status
        return self.event_type

    @property
    def strategy(self) -> str:
        """Alias for name"""
        return self.name

    @property
    def steps_done(self) -> int:
        """Return completed_steps if available, otherwise default to 0"""
        return self.completed_steps or 0

    @property
    def params(self) -> dict:
        """Additional parameters"""
        return {
            "discount_pct": self.discount_pct,
            "duration_minutes": self.duration_minutes
        }
    
    @property
    def is_dca_job(self) -> bool:
        """Check if this is a DCA job based on name."""
        return self.name.lower() in [n.lower() for n in self.DCA_JOB_NAMES]
    
    @property
    def is_liq_job(self) -> bool:
        """Check if this is a liquidity job based on name."""
        return self.name.lower() in [n.lower() for n in self.LIQ_JOB_NAMES]
    
    @property
    def is_active(self) -> bool:
        """Check if job is currently active (not finished or stopped)."""
        return self.status not in self.TERMINAL_STATUSES
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Job":
        """Create a Job from a dictionary."""
        return cls(**data)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the model to a dictionary."""
        return self.model_dump()


class CreateJobEvent(JobEvent):
    """Specific model for job creation events."""
    name: Optional[str] = ""
    coins: List[str] = []
    side: Optional[str] = ""
    discount_pct: float = 0.0
    amount: float = 0.0
    steps_total: int = 0
    duration_minutes: float = 0.0
    
    def to_job(self) -> Job:
        """Convert creation event to a Job object."""
        return Job(
            job_id=self.job_id,
            user_id=self.user_id,
            event_type=self.event_type,
            name=self.name,
            coins=self.coins,
            side=self.side,
            discount_pct=self.discount_pct,
            amount=self.amount,
            steps_total=self.steps_total,
            duration_minutes=self.duration_minutes,
            timestamp=self.timestamp,
            status="Created",
            completed_steps=0,
            orders=[]
        ) 