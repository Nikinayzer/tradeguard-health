"""
Job Event Models

This module defines the models for job events coming from Kafka.
It mirrors the Java implementation with proper deserialization logic.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Union, Optional
from datetime import datetime

from src.utils import log_util

logger = log_util.get_logger()


@dataclass
class JobEventType:
    """Base class for all job event types"""
    type_name: str = field(default="", init=False)

    @classmethod
    def from_dict(cls, data: Union[str, Dict[str, Any]]) -> 'JobEventType':
        """Factory method to create event type from dict or string"""
        if isinstance(data, str):
            # Case 1: Simple string event types
            return cls._deserialize_string_event(data)
        elif isinstance(data, dict):
            # Case 2: Complex object event types
            return cls._deserialize_object_event(data)
        else:
            logger.error(f"Expected string or object for event_type, got: {type(data)}")
            raise ValueError(f"Expected string or object for event_type, got: {type(data)}")

    @classmethod
    def _deserialize_string_event(cls, event_str: str) -> 'JobEventType':
        """Deserialize string event types"""
        if event_str == "CanceledOrders" or event_str == "CancelledOrders":
            return CanceledOrders()
        elif event_str == "Paused":
            return Paused()
        elif event_str == "Resumed":
            return Resumed()
        elif event_str == "Stopped":
            return Stopped()
        elif event_str == "Finished":
            return Finished()
        elif event_str == "StepDone":
            # For the new flat format, we'll set step_index to 0 here
            # and the actual completed_steps value is in the parent object
            return StepDone(0)
        elif event_str == "OrdersPlaced":
            # For the new flat format, we'll set orders to empty here
            # and the actual orders are in the parent object
            return OrdersPlaced([])
        elif event_str == "Created":
            # For the new flat format, we'll create an empty CreatedMeta
            # and the actual metadata is in the parent object
            return Created(CreatedMeta(
                name="",
                user_id=0,
                coins=[],
                side="",
                discount_pct=0.0,
                amount=0.0,
                steps_total=0,
                duration_minutes=0.0
            ))
        else:
            logger.warning(f"Unrecognized string variant for event_type: {event_str}")
            # Instead of raising an error, create a generic event type
            return JobEventType()

    @classmethod
    def _deserialize_object_event(cls, event_obj: Dict[str, Any]) -> 'JobEventType':
        """Deserialize complex object event types"""
        if "Created" in event_obj:
            created_data = event_obj["Created"]
            return Created(CreatedMeta(
                name=created_data.get("name", ""),
                user_id=int(created_data.get("user_id", 0)),
                coins=created_data.get("coins", []),
                side=created_data.get("side", ""),
                discount_pct=float(created_data.get("discount_pct", 0.0)),
                amount=float(created_data.get("amount", 0.0)),
                steps_total=int(created_data.get("steps_total", 0)),
                duration_minutes=float(created_data.get("duration_minutes", 0.0))
            ))
        elif "StepDone" in event_obj:
            step_index = event_obj["StepDone"]
            return StepDone(step_index)
        elif "Error" in event_obj:
            error_msg = event_obj["Error"]
            return ErrorEvent(error_msg)
        elif "OrdersPlaced" in event_obj:
            orders_data = event_obj["OrdersPlaced"]
            if not isinstance(orders_data, list):
                logger.error(f"'OrdersPlaced' should be an array, but was: {orders_data}")
                raise ValueError("'OrdersPlaced' must be an array")

            orders = [OpenOrderLog.from_dict(order) for order in orders_data]
            return OrdersPlaced(orders)
        elif "CancelledOrders" in event_obj:
            cancelled_data = event_obj["CancelledOrders"]
            if not isinstance(cancelled_data, list):
                logger.error(f"'CancelledOrders' should be an array, but was: {cancelled_data}")
                raise ValueError("'CancelledOrders' must be an array")

            cancelled = [OpenOrderLog.from_dict(order) for order in cancelled_data]
            return CancelledOrders(cancelled)
        else:
            logger.error(f"Unknown object variant in event_type: {event_obj}")
            raise ValueError(f"Unknown object variant in event_type: {event_obj}")


@dataclass
class Paused(JobEventType):
    """Job paused event"""
    type_name: str = field(default="Paused", init=False)


@dataclass
class Resumed(JobEventType):
    """Job resumed event"""
    type_name: str = field(default="Resumed", init=False)


@dataclass
class Stopped(JobEventType):
    """Job stopped event"""
    type_name: str = field(default="Stopped", init=False)


@dataclass
class Finished(JobEventType):
    """Job finished event"""
    type_name: str = field(default="Finished", init=False)


@dataclass
class CanceledOrders(JobEventType):
    """Orders canceled event (empty version)"""
    type_name: str = field(default="CanceledOrders", init=False)


@dataclass
class CreatedMeta:
    """Metadata for Created event"""
    name: str
    user_id: int
    coins: List[str]
    side: str
    discount_pct: float
    amount: float
    steps_total: int
    duration_minutes: float


@dataclass
class Created(JobEventType):
    """Job created event"""
    data: CreatedMeta
    type_name: str = field(default="Created", init=False)


@dataclass
class StepDone(JobEventType):
    """Step completed event"""
    step_index: int
    type_name: str = field(default="StepDone", init=False)


@dataclass
class ErrorEvent(JobEventType):
    """Error event"""
    error_message: str
    type_name: str = field(default="Error", init=False)


@dataclass
class OpenOrderLog:
    """Log of an open order"""
    order_id: str
    symbol: str
    side: str
    price: float
    quantity: float
    status: str
    timestamp: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OpenOrderLog':
        """Create an OpenOrderLog from a dictionary"""
        return cls(
            order_id=data.get('order_id', ''),
            symbol=data.get('symbol', ''),
            side=data.get('side', ''),
            price=float(data.get('price', 0.0)),
            quantity=float(data.get('quantity', 0.0)),
            status=data.get('status', ''),
            timestamp=data.get('timestamp', datetime.now().isoformat())
        )


@dataclass
class OrdersPlaced(JobEventType):
    """Orders placed event"""
    orders: List[OpenOrderLog]
    type_name: str = field(default="OrdersPlaced", init=False)


@dataclass
class CancelledOrders(JobEventType):
    """Orders cancelled event"""
    orders: List[OpenOrderLog]
    type_name: str = field(default="CancelledOrders", init=False)


@dataclass
class JobEvent:
    """Full job event from Kafka"""
    job_id: str
    timestamp: str
    event_type: JobEventType

    # Optional fields that might be in the flat structure
    user_id: Optional[int] = None
    completed_steps: Optional[int] = None
    orders: Optional[List[Dict[str, Any]]] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'JobEvent':
        """Create a JobEvent from a dictionary"""
        # Handle missing values
        if 'job_id' not in data:
            logger.error("Missing job_id in job event data")
            raise ValueError("Missing job_id in job event data")

        if 'event_type' not in data:
            logger.error("Missing event_type in job event data")
            raise ValueError("Missing event_type in job event data")

        # Extract optional fields from the flat structure
        user_id = data.get('user_id')
        if user_id and isinstance(user_id, str):
            try:
                user_id = int(user_id)
            except ValueError:
                logger.warning(f"Could not convert user_id to int: {user_id}")

        completed_steps = data.get('completed_steps')
        orders = data.get('orders')

        # Create the JobEvent instance
        return cls(
            job_id=data.get('job_id', ''),
            timestamp=data.get('timestamp', datetime.now().isoformat()),
            event_type=JobEventType.from_dict(data.get('event_type', {})),
            user_id=user_id,
            completed_steps=completed_steps,
            orders=orders
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert JobEvent to a dictionary for storage"""
        # Start with basic fields
        result = {
            'job_id': self.job_id,
            'timestamp': self.timestamp,
            'event_type': self.event_type.type_name
        }

        # Add user_id if available
        if self.user_id is not None:
            result['user_id'] = self.user_id

        # Add completed_steps if available
        if self.completed_steps is not None:
            result['completed_steps'] = self.completed_steps

        # Add orders if available
        if self.orders is not None:
            result['orders'] = self.orders

        # Add type-specific fields from complex event types
        if isinstance(self.event_type, Created) and hasattr(self.event_type, 'data'):
            # Only add these fields if they aren't already in the flat structure
            if self.user_id is None and self.event_type.data.user_id:
                result['user_id'] = self.event_type.data.user_id

            if self.event_type.data.name:
                result['name'] = self.event_type.data.name

            if self.event_type.data.coins:
                result['coins'] = self.event_type.data.coins

            if self.event_type.data.side:
                result['side'] = self.event_type.data.side

            if self.event_type.data.discount_pct:
                result['discount_pct'] = self.event_type.data.discount_pct

            if self.event_type.data.amount:
                result['amount'] = self.event_type.data.amount

            if self.event_type.data.steps_total:
                result['steps_total'] = self.event_type.data.steps_total

            if self.event_type.data.duration_minutes:
                result['duration_minutes'] = self.event_type.data.duration_minutes

            result['status'] = 'Created'

        elif isinstance(self.event_type, StepDone):
            # Only add step_index if completed_steps isn't already in the flat structure
            if self.completed_steps is None and self.event_type.step_index:
                result['completed_steps'] = self.event_type.step_index

        elif isinstance(self.event_type, OrdersPlaced):
            # Only add orders if they aren't already in the flat structure
            if self.orders is None and self.event_type.orders:
                result['orders'] = [vars(order) for order in self.event_type.orders]

        elif isinstance(self.event_type, CancelledOrders):
            # Add cancelled_orders (these don't have a flat field counterpart)
            result['cancelled_orders'] = [vars(order) for order in self.event_type.orders]

        elif isinstance(self.event_type, (Paused, Resumed, Stopped, Finished)):
            result['status'] = self.event_type.type_name

        return result
