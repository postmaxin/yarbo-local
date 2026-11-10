"""Local-first library and tooling for Yarbo robots over their LAN MQTT broker.

The library talks to the anonymous broker on the robot and to nothing else.
``YarboRobot`` is the entry point; ``Session`` is the protocol layer under it;
``Simulator`` stands in for a robot when there is none on the bench.
"""

from __future__ import annotations

from . import payloads
from .blackbox import FlightRecorder
from .client import YarboRobot
from .cloud import CloudBroker, CloudSessionError, CloudTransport, resolve_cloud_broker
from .exceptions import (
    CommandError,
    CommandRefusedError,
    ConnectionLostError,
    ControllerError,
    PlanRunningError,
    PlanStartError,
    PreflightError,
    ReplyTimeoutError,
    RobotNotFoundError,
    YarboError,
)
from .feedback import AreaProgress, BarrierPoints, PlanFeedback, RechargeFeedback
from .lifecycle import EventKind, FinishReason, LifecycleEvent, Phase, PlanTracker
from .models import (
    Activity,
    ChargingPoint,
    Fault,
    Feedback,
    GgaFix,
    GpsReference,
    PlanError,
    PlanSummary,
    RobotState,
    Schedule,
    SiteMap,
    Zone,
)
from .obstacles import Barrier, ObstacleTracker, Run
from .preflight import Action, Refusal
from .registry import Command, Registry
from .resolve import resolve
from .session import Session
from .simulator import Simulator
from .transport import FakeBroker, FakeTransport, MqttTransport

__version__ = "0.1.0.dev2"

__all__ = [
    "Action",
    "Activity",
    "AreaProgress",
    "Barrier",
    "BarrierPoints",
    "ChargingPoint",
    "CloudBroker",
    "CloudSessionError",
    "CloudTransport",
    "Command",
    "CommandError",
    "CommandRefusedError",
    "ConnectionLostError",
    "ControllerError",
    "EventKind",
    "FakeBroker",
    "FakeTransport",
    "Fault",
    "Feedback",
    "FinishReason",
    "FlightRecorder",
    "GgaFix",
    "GpsReference",
    "LifecycleEvent",
    "MqttTransport",
    "ObstacleTracker",
    "Phase",
    "PlanError",
    "PlanFeedback",
    "PlanRunningError",
    "PlanStartError",
    "PlanSummary",
    "PlanTracker",
    "PreflightError",
    "RechargeFeedback",
    "Refusal",
    "Registry",
    "ReplyTimeoutError",
    "RobotNotFoundError",
    "RobotState",
    "Run",
    "Schedule",
    "Session",
    "Simulator",
    "SiteMap",
    "YarboError",
    "YarboRobot",
    "Zone",
    "payloads",
    "resolve",
    "resolve_cloud_broker",
]
