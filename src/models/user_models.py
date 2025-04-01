from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, field_validator

from src.utils import log_util

logger = log_util.get_logger()


class UserLimits(BaseModel):
    """Model representing trading limits for a user."""
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

    @property
    def allowed_strategies(self) -> List[str]:
        """Convert boolean flags to list of allowed strategies"""
        strategies = []
        if self.allowDcaForce:
            strategies.append("dca")
        if self.allowLiqForce:
            strategies.append("liq")
        return strategies

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "UserLimits":
        """Create a UserLimits instance from a dictionary."""
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the model to a dictionary."""
        return self.model_dump()


