from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Dict, Any, Type, Union
import logging

from src.utils.datetime_utils import parse_timestamp, format_timestamp

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------
# Base Classes and Registry
# ------------------------------------------------------------------------------

@dataclass(frozen=True)
class JobEventType(ABC):
    type_name: str = field(init=False)

    @classmethod
    @abstractmethod
    def from_data(cls, data: Any) -> 'JobEventType':
        """
        Deserialize event-specific data into an instance of the event.
        """
        pass

    @classmethod
    def from_value(cls, data: Union[str, Dict[str, Any]]) -> 'JobEventType':
        # Case 1: Input is a simple string.
        if isinstance(data, str):
            event_class = _EVENT_TYPE_MAP.get(data)
            if event_class is None:
                logger.warning("Unrecognized event type string: %s", data)
                return UnknownEvent(raw=data)
            return event_class.from_data(data)
        # Case 2: Input is a dict.
        elif isinstance(data, dict):
            for key, value in data.items():
                event_class = _EVENT_TYPE_MAP.get(key)
                if event_class:
                    return event_class.from_data(value)
            raise ValueError(f"Unknown event type in object: {data}")
        else:
            logger.error("Expected string or dict for event type, got: %s", type(data))
            raise ValueError(f"Expected string or dict for event type, got: {type(data)}")


@dataclass(frozen=True)
class UnknownEvent(JobEventType):
    raw: Any

    def __post_init__(self):
        object.__setattr__(self, "type_name", "Unknown")

    @classmethod
    def from_data(cls, data: Any) -> 'UnknownEvent':
        return cls(raw=data)


# ------------------------------------------------------------------------------
# Concrete Event Types
# ------------------------------------------------------------------------------

# Simple events that do not require extra data.

@dataclass(frozen=True)
class Paused(JobEventType):
    def __post_init__(self):
        object.__setattr__(self, "type_name", "Paused")

    @classmethod
    def from_data(cls, data: Any) -> 'Paused':
        return cls()


@dataclass(frozen=True)
class Resumed(JobEventType):
    def __post_init__(self):
        object.__setattr__(self, "type_name", "Resumed")

    @classmethod
    def from_data(cls, data: Any) -> 'Resumed':
        return cls()


@dataclass(frozen=True)
class Stopped(JobEventType):
    def __post_init__(self):
        object.__setattr__(self, "type_name", "Stopped")

    @classmethod
    def from_data(cls, data: Any) -> 'Stopped':
        return cls()


@dataclass(frozen=True)
class Finished(JobEventType):
    def __post_init__(self):
        object.__setattr__(self, "type_name", "Finished")

    @classmethod
    def from_data(cls, data: Any) -> 'Finished':
        return cls()


@dataclass(frozen=True)
class CanceledOrders(JobEventType):
    def __post_init__(self):
        object.__setattr__(self, "type_name", "CanceledOrders")

    @classmethod
    def from_data(cls, data: Any) -> 'CanceledOrders':
        return cls()


# Complex event: Created

@dataclass(frozen=True)
class CreatedMeta:
    name: str
    user_id: int
    coins: List[str]
    side: str
    discount_pct: float
    amount: float
    steps_total: int
    duration_minutes: float


@dataclass(frozen=True)
class Created(JobEventType):
    data: CreatedMeta

    def __post_init__(self):
        object.__setattr__(self, "type_name", "Created")

    @classmethod
    def from_data(cls, data: Any) -> 'Created':
        if not isinstance(data, dict):
            data = {}
        meta = CreatedMeta(
            name=data.get("name", ""),
            user_id=int(data.get("user_id", 0)),
            coins=data.get("coins", []),
            side=data.get("side", ""),
            discount_pct=float(data.get("discount_pct", 0.0)),
            amount=float(data.get("amount", 0.0)),
            steps_total=int(data.get("steps_total", 0)),
            duration_minutes=float(data.get("duration_minutes", 0.0))
        )
        return cls(data=meta)


# Complex event: StepDone

@dataclass(frozen=True)
class StepDone(JobEventType):
    step_index: int

    def __post_init__(self):
        object.__setattr__(self, "type_name", "StepDone")

    @classmethod
    def from_data(cls, data: Any) -> 'StepDone':
        if isinstance(data, int):
            return cls(step_index=data)
        elif isinstance(data, dict):
            return cls(step_index=int(data.get("step_index", 0)))
        else:
            logger.error("Invalid data for StepDone: %s", data)
            raise ValueError("Invalid data for StepDone")


# Complex event: ErrorEvent

@dataclass(frozen=True)
class ErrorEvent(JobEventType):
    error_message: str

    def __post_init__(self):
        object.__setattr__(self, "type_name", "Error")

    @classmethod
    def from_data(cls, data: Any) -> 'ErrorEvent':
        if isinstance(data, str):
            return cls(error_message=data)
        elif isinstance(data, dict):
            return cls(error_message=str(data.get("message", "")))
        else:
            logger.error("Invalid data for ErrorEvent: %s", data)
            raise ValueError("Invalid data for ErrorEvent")


# Complex event: OrdersPlaced

@dataclass(frozen=True)
class OpenOrderLog:
    order_id: str
    symbol: str
    side: str
    price: float
    quantity: float
    status: str
    timestamp: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OpenOrderLog':
        return cls(
            order_id=data.get('order_id', ''),
            symbol=data.get('symbol', ''),
            side=data.get('side', ''),
            price=float(data.get('price', 0.0)),
            quantity=float(data.get('quantity', 0.0)),
            status=data.get('status', ''),
            timestamp=data.get('timestamp', datetime.now().isoformat())
        )


@dataclass(frozen=True)
class OrdersPlaced(JobEventType):
    orders: List[OpenOrderLog]

    def __post_init__(self):
        object.__setattr__(self, "type_name", "OrdersPlaced")

    @classmethod
    def from_data(cls, data: Any) -> 'OrdersPlaced':
        if not isinstance(data, list):
            logger.error("'OrdersPlaced' should be a list, but got: %s", data)
            raise ValueError("'OrdersPlaced' must be a list")
        orders = [OpenOrderLog.from_dict(order) for order in data]
        return cls(orders=orders)


# ------------------------------------------------------------------------------
# Registry for Event Type Dispatching
# ------------------------------------------------------------------------------

_EVENT_TYPE_MAP: Dict[str, Type[JobEventType]] = {
    "Paused": Paused,
    "Resumed": Resumed,
    "Stopped": Stopped,
    "Finished": Finished,
    "CanceledOrders": CanceledOrders,
    "Created": Created,
    "StepDone": StepDone,
    "Error": ErrorEvent,
    "OrdersPlaced": OrdersPlaced,
}


# ------------------------------------------------------------------------------
# Full JobEvent Model
# ------------------------------------------------------------------------------

@dataclass
class JobEvent:
    job_id: int
    timestamp: datetime  # Now explicitly a datetime object
    type: JobEventType

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'JobEvent':
        if 'job_id' not in data:
            logger.error("Missing job_id in job event data")
            raise ValueError("Missing job_id in job event data")
        if 'update_type' not in data:
            logger.error("Missing update_type in job event data")
            raise ValueError("Missing update_type in job event data")

        event = JobEventType.from_value(data['update_type'])
        
        # Handle timestamp parsing from string to datetime
        timestamp_str = data.get('timestamp', datetime.now(timezone.utc).isoformat())
        # Always use parse_timestamp to ensure timezone awareness
        timestamp = parse_timestamp(timestamp_str)
            
        return cls(
            job_id=data['job_id'],
            timestamp=timestamp,
            type=event
        )

    def to_dict(self) -> Dict[str, Any]:
        # Format the datetime to ISO string with Z timezone
        timestamp_str = format_timestamp(self.timestamp)
        
        result = {
            'job_id': self.job_id,
            'timestamp': timestamp_str,
            'update_type': self.type.type_name
        }
        # Optionally include extra fields for specific event types:
        if isinstance(self.type, Created):
            meta = self.type.data
            result.update({
                'name': meta.name,
                'coins': meta.coins,
                'side': meta.side,
                'discount_pct': meta.discount_pct,
                'amount': meta.amount,
                'steps_total': meta.steps_total,
                'duration_minutes': meta.duration_minutes,
                'status': 'Created'
            })
        elif isinstance(self.type, StepDone):
            result['completed_steps'] = self.type.step_index
        elif isinstance(self.type, OrdersPlaced):
            result['orders'] = [vars(order) for order in self.type.orders]
        elif isinstance(self.type, (Paused, Resumed, Stopped, Finished)):
            result['status'] = self.type.type_name
        elif isinstance(self.type, ErrorEvent):
            result['error_message'] = self.type.error_message

        return result
