from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional
from pydantic import BaseModel

from src.utils.datetime_utils import parse_timestamp, format_timestamp
from src.utils import log_util

logger = log_util.get_logger()


class PositionUpdateType(str, Enum):
    """Position update types that can be set on a Position object."""
    INCREASED = "Increased"
    DECREASED = "Decreased"
    CLOSED = "Closed"
    SNAPSHOT = "Snapshot"


class Position(BaseModel):
    """Model representing a trading position with its full state."""
    venue: str
    symbol: str
    side: str
    qty: float
    usdt_amt: float
    entry_price: float
    mark_price: float
    liquidation_price: Optional[float] = None
    unrealized_pnl: float
    cur_realized_pnl: float
    cum_realized_pnl: float
    leverage: float
    timestamp: datetime
    account_name: str
    user_id: int
    update_type: PositionUpdateType

    class Config:
        json_encoders = {
            datetime: format_timestamp
        }

    @property
    def position_key(self) -> str:
        """Generate a unique key for this position based on venue and symbol."""
        return f"{self.venue}_{self.symbol}"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Position':
        """Create a Position from a dictionary."""
        try:
            position_data = data.copy()

            # Extract position data if nested
            if 'position' in position_data:
                position_details = position_data.pop('position')
                position_data.update(position_details)

            # Convert string values to appropriate types
            if 'qty' in position_data and isinstance(position_data['qty'], str):
                position_data['qty'] = float(position_data['qty'])

            if 'usdt_amt' in position_data and isinstance(position_data['usdt_amt'], str):
                position_data['usdt_amt'] = float(position_data['usdt_amt'])

            if 'entry_price' in position_data and isinstance(position_data['entry_price'], str):
                position_data['entry_price'] = float(position_data['entry_price'])

            if 'mark_price' in position_data and isinstance(position_data['mark_price'], str):
                position_data['mark_price'] = float(position_data['mark_price'])

            if 'liquidation_price' in position_data and isinstance(position_data['liquidation_price'], str):
                position_data['liquidation_price'] = float(position_data['liquidation_price'])

            if 'unrealized_pnl' in position_data and isinstance(position_data['unrealized_pnl'], str):
                position_data['unrealized_pnl'] = float(position_data['unrealized_pnl'])

            if 'cur_realized_pnl' in position_data and isinstance(position_data['cur_realized_pnl'], str):
                position_data['cur_realized_pnl'] = float(position_data['cur_realized_pnl'])

            if 'cum_realized_pnl' in position_data and isinstance(position_data['cum_realized_pnl'], str):
                position_data['cum_realized_pnl'] = float(position_data['cum_realized_pnl'])

            if 'leverage' in position_data and isinstance(position_data['leverage'], str):
                position_data['leverage'] = float(position_data['leverage'])

            if 'timestamp' in position_data and isinstance(position_data['timestamp'], str):
                position_data['timestamp'] = parse_timestamp(position_data['timestamp'])

            return cls(**position_data)
        except Exception as e:
            logger.error(f"Error creating Position from dict: {e}")
            raise

    def to_dict(self) -> Dict[str, Any]:
        """Convert the model to a dictionary."""
        data = self.model_dump()

        data['timestamp'] = format_timestamp(self.timestamp)

        return data
