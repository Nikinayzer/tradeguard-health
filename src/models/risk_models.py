from datetime import datetime, timedelta, timezone
from enum import Enum
import hashlib
from typing import List, Dict, Any, Optional,Literal, cast
from pydantic import BaseModel, Field, computed_field

from src.utils import log_util

logger = log_util.get_logger()


class RiskCategory(str, Enum):
    """Enumeration of risk categories"""
    OVERCONFIDENCE = "overconfidence"
    FOMO = "fomo"
    LOSS_BEHAVIOR = "loss_behavior"  # loss-aversion + loss-seeking


class RiskLevel(str, Enum):
    """Enumeration of risk severity levels."""
    NONE = "None"
    LOW = "low"  # < 30
    MEDIUM = "medium"  # 30-70
    HIGH = "high"  # > 70
    CRITICAL = "critical"  # > 90


def default_category_weights() -> Dict[RiskCategory, float]:
    equal_weight = 1.0 / len(RiskCategory)
    return cast(Dict[RiskCategory, float], {
        category: equal_weight for category in RiskCategory
    })


class BasePattern(BaseModel):
    """Base model for a pattern."""
    pattern_id: str
    job_id: Optional[List[int]] = None
    position_key: Optional[str] = None
    description: Optional[str] = None
    message: str
    category_weights: Optional[Dict[RiskCategory, float]] = Field(default_factory=default_category_weights)
    details: Optional[Dict[str, Any]] = None
    start_time: Optional[datetime] = Field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: Optional[datetime] = None
    show_if_not_consumed: bool = True  # some atomic patterns should not be shown if not consumed
    is_composite: bool = False
    unique: bool = False  # if True, only one instance of this pattern can exist at a time
    ttl_minutes: Optional[int] = 60  # Time-to-live in minutes, None = no expiration

    @property
    def category(self) -> RiskCategory:
        """Get the primary category for this pattern."""
        weights = self.category_weights
        return max(weights.items(), key=lambda x: x[1])[0]

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
        """Generate a unique ID hash for pattern tracking."""
        if self.is_composite:
            data = [
                self.pattern_id,
                self.start_time.isoformat() if self.start_time else '',
                '_'.join(sorted(self.component_patterns)) if hasattr(self, 'component_patterns') else '',
                f"{self.confidence:.2f}" if hasattr(self, 'confidence') else '',
                '_'.join(
                    f"{k}:{v:.2f}" for k, v in sorted(self.category_weights.items())) if self.category_weights else ''
            ]
        else:
            data = [
                self.pattern_id,
                self.start_time.isoformat() if self.start_time else '',
                '_'.join(map(str, self.job_id)) if self.job_id else '',
                self.position_key or '',
                f"{self.severity:.2f}" if hasattr(self, 'severity') else '',
                '_'.join(
                    f"{k}:{v:.2f}" for k, v in sorted(self.category_weights.items())) if self.category_weights else ''
            ]

        data_str = '||'.join(filter(None, data))
        hash_obj = hashlib.md5(data_str.encode())
        short_hash = hash_obj.hexdigest()[:12]

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
