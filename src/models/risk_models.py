from datetime import datetime
from enum import Enum
from typing import List, Dict, Any, Optional, Union, Literal
from pydantic import BaseModel, Field, field_validator

from src.utils import log_util

logger = log_util.get_logger()


class RiskCategory(str, Enum):
    """Enumeration of risk categories"""
    OVERTRADING = "overtrading"
    FOMO = "fomo"
    SUNK_COST = "sunk_cost"
    POSITION_SIZE = "position_size"
    TIME_PATTERN = "time_pattern"
    PORTFOLIO_EXPOSURE = "portfolio_exposure"
    MARKET_VOLATILITY = "market_volatility"
    LIQUIDITY = "liquidity"
    EXECUTION = "execution"


class RiskLevel(str, Enum):
    """Enumeration of risk severity levels."""
    NONE = "None"
    LOW = "low"  # < 30
    MEDIUM = "medium"  # 30-70
    HIGH = "high"  # > 70
    CRITICAL = "critical"  # > 90


class Pattern(BaseModel):
    """Model for individual pattern."""
    pattern_id: str
    job_id: Optional[List[int]] = None
    message: str
    confidence: float
    category_weights: Dict[RiskCategory, float]
    details: Optional[Dict[str, Any]] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Pattern":
        """Create a Trigger from a dictionary."""
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the trigger to a dictionary."""
        return self.model_dump()


class RiskRepost(BaseModel):
    """Base model for risk alerts sent to Kafka."""
    event_type: Literal["RiskReport"] = "RiskReport"
    user_id: int
    job_id: Optional[int]
    top_risk_level: RiskLevel
    top_risk_confidence: float = Field(ge=0.0, le=100.0)
    top_risk_type: RiskCategory
    category_scores: Dict[RiskCategory, float]
    patterns: List[Pattern]
    decay_params: Dict[str, Any]
    timestamp: datetime = Field(default_factory=datetime.now)

    class Config:
        json_encoders = {
            datetime: lambda dt: dt.isoformat(),
            Enum: lambda e: e.value
        }

    @property
    def has_patterns(self) -> bool:
        """Check if the alert contains any patterns."""
        return len(self.patterns) > 0
