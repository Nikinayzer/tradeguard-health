from datetime import datetime, timezone
from typing import List, Optional, Dict, Any, ClassVar
from pydantic import BaseModel, Field, field_validator

from src.models.job_updates import (
    JobEvent, StepDone, OrdersPlaced, Finished, 
    Paused, Resumed, ErrorEvent, Created
)
from src.utils import log_util
from src.utils.datetime_utils import parse_timestamp, format_timestamp

logger = log_util.get_logger()


class Job(BaseModel):
    """Model representing a trading job with its full state."""
    job_id: int
    user_id: int
    name: Optional[str] = ""
    coins: List[str] = []
    side: Optional[str] = ""
    discount_pct: float = 0.0
    amount: float = 0.0
    steps_total: int = 0
    duration_minutes: float = 0.0
    timestamp: datetime
    last_updated: datetime
    status: Optional[str] = ""
    completed_steps: Optional[int] = 0
    orders: Optional[List[Dict[str, Any]]] = []

    DCA_JOB_NAMES: ClassVar[List[str]] = ["dca", "Dca", "DCA"]
    LIQ_JOB_NAMES: ClassVar[List[str]] = ["liq", "Liq", "LIQ"]
    TERMINAL_STATUSES: ClassVar[List[str]] = ["Finished", "Stopped"]

    class Config:
        json_encoders = {
            datetime: format_timestamp
        }

    @property
    def id(self) -> int:
        """Alias for job_id"""
        return self.job_id

    @property
    def created_at(self) -> datetime:
        """Return timestamp as datetime"""
        return self.timestamp

    @property
    def updated_at(self) -> datetime:
        """Return the last update timestamp"""
        return self.last_updated

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
        job_data = data.copy()
        
        if 'last_updated' not in job_data:
            job_data['last_updated'] = job_data.get('timestamp', '')
            
        if 'timestamp' in job_data and isinstance(job_data['timestamp'], str):
            job_data['timestamp'] = parse_timestamp(job_data['timestamp'])
            
        if 'last_updated' in job_data and isinstance(job_data['last_updated'], str):
            job_data['last_updated'] = parse_timestamp(job_data['last_updated'])
            
        return cls(**job_data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the model to a dictionary."""
        data = self.model_dump()
        
        data['timestamp'] = format_timestamp(self.timestamp)
        data['last_updated'] = format_timestamp(self.last_updated)
        
        return data

    @classmethod
    def create_from_event(cls, event: JobEvent) -> 'Job':
        """Create a new job from a Created event."""
        if not isinstance(event.type, Created):
            raise ValueError("Can only create jobs from Created events")
            
        return cls(
            job_id=event.job_id,
            user_id=event.type.data.user_id,
            name=event.type.data.name,
            coins=event.type.data.coins,
            side=event.type.data.side,
            discount_pct=event.type.data.discount_pct,
            amount=event.type.data.amount,
            steps_total=event.type.data.steps_total,
            duration_minutes=event.type.data.duration_minutes,
            timestamp=event.timestamp,
            last_updated=event.timestamp,
            status="Created"
        )

    def apply_event(self, event: JobEvent) -> None:
        """Update job state based on an event."""
        self.last_updated = event.timestamp
        
        if isinstance(event.type, StepDone):
            self.completed_steps = event.type.step_index
            self.status = "In Progress"
            
        elif isinstance(event.type, OrdersPlaced):
            new_orders = [vars(order) for order in event.type.orders]
            self.orders.extend(new_orders)
            
        elif isinstance(event.type, Finished):
            self.status = event.type.type_name
            
        elif isinstance(event.type, Paused):
            self.status = event.type.type_name
            
        elif isinstance(event.type, Resumed):
            self.status = event.type.type_name
            
        elif isinstance(event.type, ErrorEvent):
            self.status = "Error"
            
        else:
            logger.warning(f"Unhandled event type: {event.type.__class__.__name__}")
