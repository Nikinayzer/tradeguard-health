"""
Models package
Provides domain models for all entities in the system.
"""

from src.models.job_models import Job, JobEvent as PydanticJobEvent, CreateJobEvent
from src.models.user_models import UserLimits, UserState
from src.models.risk_models import Risk, RiskReport, Trigger, RiskType, RiskLevel
from src.models.event_mapper import EventMapper

from src.models.models import Job, UserLimits, RiskReport, Risk, Trigger

# For backward compatibility with existing code using dataclass-based events
from src.models.job_events import (
    JobEvent, JobEventType, 
    Created, Paused, Resumed, Stopped, Finished,
    StepDone, OrdersPlaced, CancelledOrders, CanceledOrders,
    ErrorEvent, OpenOrderLog, CreatedMeta
)

__all__ = [
    'Job', 'UserLimits', 'RiskReport', 'Risk', 'Trigger',

    'JobEvent', 'JobEventType',
    'Created', 'CreatedMeta', 'StepDone', 'OrdersPlaced', 'CancelledOrders',
    'Paused', 'Resumed', 'Stopped', 'Finished', 'ErrorEvent', 'OpenOrderLog'
]
