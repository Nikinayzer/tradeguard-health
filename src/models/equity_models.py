from datetime import datetime
from typing import Dict, Any, Optional
from pydantic import BaseModel

from src.utils.datetime_utils import parse_timestamp, format_timestamp
from src.utils import log_util

logger = log_util.get_logger()


class Equity(BaseModel):
    """Model representing a user's account equity."""
    user_id: int
    account_name: str
    venue: str
    timestamp: datetime
    wallet_balance: float
    available_balance: float
    total_unrealized_pnl: float
    bnb_balance_usdt: Optional[float] = None
    
    class Config:
        json_encoders = {
            datetime: format_timestamp
        }
    
    @property
    def equity_key(self) -> str:
        """Generate a unique key for this equity based on venue."""
        return f"{self.user_id}_{self.venue}"
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Equity':
        """Create an Equity object from a dictionary."""
        try:
            equity_data = data.copy()

            if 'equity' in equity_data:
                equity_details = equity_data.pop('equity')
                equity_data.update(equity_details)

            for field in ['wallet_balance', 'available_balance', 'total_unrealized_pnl']:
                if field in equity_data and isinstance(equity_data[field], str):
                    equity_data[field] = float(equity_data[field])

            if 'bnb_balance_usdt' in equity_data and equity_data['bnb_balance_usdt'] is not None:
                if isinstance(equity_data['bnb_balance_usdt'], str):
                    equity_data['bnb_balance_usdt'] = float(equity_data['bnb_balance_usdt'])

            if 'timestamp' in equity_data and isinstance(equity_data['timestamp'], str):
                equity_data['timestamp'] = parse_timestamp(equity_data['timestamp'])
                
            return cls(**equity_data)
        except Exception as e:
            logger.error(f"Error creating Equity from dict: {e}")
            raise

    def to_dict(self) -> Dict[str, Any]:
        """Convert the model to a dictionary."""
        data = self.model_dump()
        
        data['timestamp'] = format_timestamp(self.timestamp)
        
        return data 