import logging
from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

from src.utils import log_util

logger = log_util.get_logger()


class Job(BaseModel):
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
        return self.completed_steps

    @property
    def params(self) -> dict:
        """Additional parameters"""
        return {
            "discount_pct": self.discount_pct,
            "duration_minutes": self.duration_minutes
        }


class UserLimits(BaseModel):
    id: int
    userId: int
    maxSingleJobLimit: float
    maxDailyTradingLimit: float
    maxPortfolioRisk: float
    maxConcurrentOrders: int
    maxDailyTrades: int
    tradingCooldown: int
    allowDcaForce: bool
    allowLiqForce: bool
    dailyLossLimit: float
    maxConsecutiveLosses: int
    maxDailyBalanceChange: float
    volatilityLimit: float
    liquidityThreshold: float

    @property
    def user_id(self) -> int:
        """Alias for userId"""
        return self.userId

    @property
    def max_daily_trades(self) -> int:
        """Alias for maxDailyTrades"""
        return self.maxDailyTrades

    @property
    def max_daily_volume(self) -> float:
        """Alias for maxDailyTradingLimit"""
        return self.maxDailyTradingLimit

    @property
    def min_trade_interval_minutes(self) -> int:
        """Alias for tradingCooldown"""
        return self.tradingCooldown

    @property
    def max_trade_interval_minutes(self) -> int:
        """Default value for max trade interval"""
        return 1440  # 24 hours

    @property
    def max_concurrent_jobs(self) -> int:
        """Alias for maxConcurrentOrders"""
        return self.maxConcurrentOrders

    @property
    def max_daily_loss(self) -> float:
        """Alias for dailyLossLimit"""
        return self.dailyLossLimit

    @property
    def max_position_size(self) -> float:
        """Alias for maxSingleJobLimit"""
        return self.maxSingleJobLimit

    # @property
    # def max_leverage(self) -> float:
    #     """Default value for max leverage"""
    #     return 1.0

    # @property
    # def allowed_strategies(self) -> List[str]:
    #     """Convert boolean flags to list of allowed strategies"""
    #     strategies = []
    #     if self.allowDcaForce:
    #         strategies.append("dca")
    #     if self.allowLiqForce:
    #         strategies.append("liq")
    #     return strategies

    # @property
    # def allowed_coins(self) -> List[str]:
    #     """Default list of allowed coins"""
    #     return ["BTC", "ETH", "DOGE"]  # Add more as needed


class Risk(BaseModel):
    type: str
    level: float = Field(description="Risk level as a percentage (0-100)")
    triggers: List[Dict[str, Any]] = Field(description="List of triggers that caused this risk")


class Trigger(BaseModel):
    type: str
    value: float
    description: str


class RiskReport(BaseModel):
    user_id: int
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
    type: str = "overtrading"
    level: float = Field(description="Overall risk level as a percentage (0-100)")
    triggers: List[Dict[str, Any]] = Field(description="List of all triggers that caused risks")
