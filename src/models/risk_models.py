from datetime import datetime, timedelta, timezone
from enum import Enum
import hashlib
from typing import List, Dict, Any, Optional, Union, Literal
from pydantic import BaseModel, Field, field_validator, computed_field

from src.utils import log_util

logger = log_util.get_logger()


class RiskCategory(str, Enum):
    """Enumeration of risk categories"""
    OVERCONFIDENCE = "overconfidence"
    FOMO = "fomo"
    LOSS_BEHAVIOR = "loss_behavior"  # loss-aversion + loss-seeking
    SUNK_COST = "sunk_cost"


class RiskLevel(str, Enum):
    """Enumeration of risk severity levels."""
    NONE = "None"
    LOW = "low"  # < 30
    MEDIUM = "medium"  # 30-70
    HIGH = "high"  # > 70
    CRITICAL = "critical"  # > 90


class BasePattern(BaseModel):
    """Base model for a pattern."""
    pattern_id: str
    job_id: Optional[List[int]] = None
    positions_key: Optional[List[str]] = None
    message: str
    category_weights: Optional[Dict[RiskCategory, float]] = None
    details: Optional[Dict[str, Any]] = None
    start_time: Optional[datetime] = Field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: Optional[datetime] = None
    show_if_not_consumed: bool = True  # some atomic patterns should not be shown if not consumed
    is_composite: bool = False
    unique: bool = False # if True, only one instance of this pattern can exist at a time
    ttl_minutes: Optional[int] = None  # Time-to-live in minutes, None = no expiration

    @property
    def is_active(self) -> bool:
        """Check if the pattern is still active based on TTL."""
        if not self.ttl_minutes:
            return True
            
        if not self.start_time:
            return False
            
        expiration_time = self.start_time + timedelta(minutes=self.ttl_minutes)
        return datetime.now(timezone.utc) < expiration_time

    @property
    def duration_minutes(self) -> Optional[float]:
        """Calculate pattern duration in minutes, if applicable."""
        if not self.end_time or not self.start_time:
            return None
        return (self.end_time - self.start_time).total_seconds() / 60

    @computed_field
    @property
    def internal_id(self) -> str:
        """Generate a shorter, cleaner unique ID hash for pattern tracking."""
        data = f"{self.pattern_id}_{self.start_time.isoformat() if self.start_time else ''}"
        if self.job_id:
            data += f"_{'_'.join(map(str, self.job_id))}"

        # Create a short hash (first 8 chars of md5)
        hash_obj = hashlib.md5(data.encode())
        short_hash = hash_obj.hexdigest()[:8]
        # Format: pattern_type:short_hash (e.g., "daily_limit:a1b2c3d4")
        pattern_type = self.pattern_id.split('_')[0] if '_' in self.pattern_id else self.pattern_id
        return f"{pattern_type}:{short_hash}"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BasePattern":
        """Create a Trigger from a dictionary."""
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the trigger to a dictionary."""
        return self.model_dump()


class AtomicPattern(BasePattern):
    """Model for atomic patterns."""
    severity: float
    consumed: bool = False

    @property
    def confidence(self) -> float:
        """Legacy compatibility property."""
        return self.severity


class CompositePattern(BasePattern):
    """Model for composite patterns."""
    confidence: float
    component_patterns: List[str]


class RiskRepost(BaseModel):
    """Base model for risk alerts sent to Kafka."""
    event_type: Literal["RiskReport"] = "RiskReport"
    user_id: int
    top_risk_level: RiskLevel
    top_risk_confidence: float = Field(ge=0.0, le=100.0)
    top_risk_type: RiskCategory
    category_scores: Dict[RiskCategory, float]
    patterns: List[AtomicPattern]
    composite_patterns: List[CompositePattern]
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    atomic_patterns_number: int = 0
    composite_patterns_number: int = 0
    consumed_patterns_number: int = 0

    class Config:
        json_encoders = {
            datetime: lambda dt: dt.isoformat(),
            Enum: lambda e: e.value
        }

    @property
    def has_patterns(self) -> bool:
        """Check if the alert contains any patterns."""
        return len(self.patterns) > 0
