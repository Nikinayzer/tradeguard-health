from datetime import datetime
from enum import Enum
from typing import List, Dict, Any, Optional, Union, Literal
from pydantic import BaseModel, Field, field_validator

from src.utils import log_util

logger = log_util.get_logger()


class RiskType(str, Enum):
    """Enumeration of risk types for consistent naming."""
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


class TriggerDetails(BaseModel):
    """Base model for trigger details. Subclassed by specific trigger types."""
    pass


class SingleJobLimitTrigger(TriggerDetails):
    """Details for a single job limit trigger."""
    amount: float = Field(description="Amount of the current job")
    limit: float = Field(description="Maximum allowed amount per job")
    ratio: float = Field(description="Ratio of amount to limit")


class DailyTradesLimitTrigger(TriggerDetails):
    """Details for a daily trades limit trigger."""
    trade_count: int = Field(description="Current number of trades today")
    limit: int = Field(description="Maximum allowed trades per day")
    ratio: float = Field(description="Ratio of trade count to limit")


class DailyVolumeLimitTrigger(TriggerDetails):
    """Details for a daily volume limit trigger."""
    daily_volume: float = Field(description="Current daily trading volume")
    limit: float = Field(description="Maximum allowed volume per day")
    ratio: float = Field(description="Ratio of volume to limit")


class TradeCooldownTrigger(TriggerDetails):
    """Details for a trade cooldown period trigger."""
    minutes_since_last_trade: float = Field(description="Minutes since last trade")
    cooldown_minutes: int = Field(description="Required cooldown period in minutes")
    cooldown_remaining_minutes: float = Field(description="Minutes remaining in cooldown period")


class ConcurrentJobsTrigger(TriggerDetails):
    """Details for a concurrent jobs limit trigger."""
    open_jobs_count: int = Field(description="Current number of open jobs")
    limit: int = Field(description="Maximum allowed concurrent jobs")
    ratio: float = Field(description="Ratio of open jobs to limit")


class Trigger(BaseModel):
    """Model for individual risk triggers."""
    job_id: Optional[int] = None
    message: str
    type: str
    details: Optional[TriggerDetails] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Trigger":
        """Create a Trigger from a dictionary."""
        return cls(**data)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the trigger to a dictionary."""
        return self.model_dump()


class Risk(BaseModel):
    """Model for risk assessment results."""
    type: str
    level: float = Field(description="Risk level as a percentage (0-100)")
    triggers: List[Dict[str, Any]] = Field(description="List of triggers that caused this risk")
    
    @property
    def risk_level_category(self) -> RiskLevel:
        """Get the risk level category based on numeric level."""
        if self.level >= 90:
            return RiskLevel.CRITICAL
        elif self.level >= 70:
            return RiskLevel.HIGH
        elif self.level >= 30:
            return RiskLevel.MEDIUM
        else:
            return RiskLevel.LOW
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Risk":
        """Create a Risk from a dictionary."""
        return cls(**data)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the risk to a dictionary."""
        return self.model_dump()


class RiskAlert(BaseModel):
    """Base model for risk alerts sent to Kafka."""
    event_type: Literal["RiskAlert"] = "RiskAlert"
    alert_type: str  
    timestamp: str
    user_id: int
    job_id: int
    job_timestamp: str
    decay_params: Dict[str, Any]


class EarlyAlert(RiskAlert):
    """Model for early alerts (for critical/high risks)."""
    alert_type: Literal["early"] = "early"
    risk_category: str
    risk_level: str
    confidence: float
    evaluator_id: str
    triggers: List[Dict[str, Any]]  # Now contains detailed trigger info
    risk_signature: str


class AggregatedAlert(RiskAlert):
    """Model for aggregated alerts (summary of all evaluators)."""
    alert_type: Literal["aggregated"] = "aggregated"
    evaluator_count: int
    highest_risk: Dict[str, Any]
    risks: List[Dict[str, Any]]
    

class RiskReport(BaseModel):
    """Represents a complete risk analysis report."""
    event_type: Literal["RiskReport"] = "RiskReport"
    user_id: int
    job_id: Optional[int]
    timestamp: datetime = Field(default_factory=datetime.now)
    risk_level: RiskLevel
    risk_type: RiskType
    confidence: float = Field(ge=0.0, le=1.0)
    triggers: List[Trigger]
    evidence: List[Dict[str, Any]] = Field(default_factory=list)
    evaluator_id: str
    
    @property
    def has_triggers(self) -> bool:
        """Check if the report contains any triggers."""
        return len(self.triggers) > 0
    
    @property
    def risk_level_category(self) -> RiskLevel:
        """Get the risk level category based on numeric level."""
        if self.confidence >= 0.7:
            return RiskLevel.CRITICAL
        elif self.confidence >= 0.5:
            return RiskLevel.HIGH
        elif self.confidence >= 0.3:
            return RiskLevel.MEDIUM
        elif self.confidence > 0:
            return RiskLevel.LOW
        return RiskLevel.NONE
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RiskReport":
        """Create a RiskReport from a dictionary."""
        return cls(**data)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the report to a dictionary."""
        return self.model_dump()
    
    @classmethod
    def create_empty(cls, user_id: int) -> "RiskReport":
        """Create an empty risk report with no triggers."""
        return cls(
            user_id=user_id,
            risk_level=RiskLevel.NONE,
            risk_type=RiskType.OVERTRADING,
            confidence=0.0,
            triggers=[],
            evidence=[],
            evaluator_id="system"
        ) 