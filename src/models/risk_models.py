from datetime import datetime
from enum import Enum
from typing import List, Dict, Any, Optional, Union
from pydantic import BaseModel, Field, field_validator

from src.utils import log_util

logger = log_util.get_logger()


class RiskType(str, Enum):
    """Enumeration of risk types for consistent naming."""
    OVERTRADING = "overtrading"
    PORTFOLIO_EXPOSURE = "portfolio_exposure"
    MARKET_VOLATILITY = "market_volatility"
    LIQUIDITY = "liquidity"
    EXECUTION = "execution"


class RiskLevel(str, Enum):
    """Enumeration of risk severity levels."""
    LOW = "low"  # < 30
    MEDIUM = "medium"  # 30-70
    HIGH = "high"  # > 70
    CRITICAL = "critical"  # > 90


class Trigger(BaseModel):
    """Model for individual risk triggers."""
    job_id: Optional[int] = None
    message: str
    type: str
    details: Dict[str, Any] = Field(default_factory=dict)
    
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


class RiskReport(BaseModel):
    """Comprehensive risk report for a user or job."""
    user_id: int
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
    type: str = RiskType.OVERTRADING
    level: float = Field(description="Overall risk level as a percentage (0-100)")
    triggers: List[Dict[str, Any]] = Field(description="List of all triggers that caused risks", default_factory=list)
    
    @property
    def has_triggers(self) -> bool:
        """Check if the report contains any triggers."""
        return len(self.triggers) > 0
    
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
            level=0.0,
            triggers=[]
        ) 