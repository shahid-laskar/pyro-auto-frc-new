"""Execution context abstraction for pyro_auto_frc.

Ensures that scheduler, admin API, population, and dispatch all operate
on a unified, immutable execution context with a single resolved zone selection.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Literal, Optional, Tuple

from app.config import settings
from app.zones import CIRCLE_METADATA, InvalidZoneError, ZoneSelection, resolve_zones

ExecutionSource = Literal["SCHEDULED", "MANUAL_API"]
VALID_SOURCES: frozenset[str] = frozenset({"SCHEDULED", "MANUAL_API"})


@dataclass(frozen=True)
class ExecutionContext:
    """Immutable execution context for a single run of population or dispatch.

    Attributes:
        execution_id: Unique correlation identifier for the execution.
        source: Trigger origin ('SCHEDULED' or 'MANUAL_API').
        selection: The resolved, validated ZoneSelection.
    """

    execution_id: str
    source: ExecutionSource
    selection: ZoneSelection

    def __post_init__(self) -> None:
        if not self.execution_id or not str(self.execution_id).strip():
            raise ValueError("execution_id must be a non-empty string.")
        if self.source not in VALID_SOURCES:
            raise ValueError(
                f"Invalid execution source '{self.source}'. Must be one of: {', '.join(sorted(VALID_SOURCES))}"
            )
        if not isinstance(self.selection, ZoneSelection):
            raise TypeError(
                f"selection must be an instance of ZoneSelection, got {type(self.selection).__name__}"
            )

    @property
    def mode(self) -> Literal["ALL", "FILTERED"]:
        """Operating mode ('ALL' or 'FILTERED')."""
        return self.selection.mode

    @property
    def zone_codes(self) -> Tuple[str, ...]:
        """Tuple of active zone codes (e.g. ('NZ',) or ('ALL',))."""
        return self.selection.zone_codes

    @property
    def circle_codes(self) -> Optional[Tuple[int, ...]]:
        """Tuple of allowed circle IDs if FILTERED, or None if ALL."""
        return self.selection.circle_codes

    @property
    def circle_count(self) -> int:
        """Total number of active circles covered by this execution."""
        if self.selection.circle_codes is not None:
            return len(self.selection.circle_codes)
        return len(CIRCLE_METADATA)

    def is_circle_allowed(self, circle_code: int | str) -> bool:
        """Check if a circle ID is allowed under this context."""
        return self.selection.is_circle_allowed(circle_code)

    def to_dict(self) -> dict:
        """Serialize context metadata for logging and API responses."""
        return {
            "execution_id": self.execution_id,
            "source": self.source,
            "mode": self.mode,
            "effective_zones": list(self.zone_codes),
            "circle_codes": list(self.circle_codes) if self.circle_codes is not None else None,
            "circles_count": self.circle_count,
        }

    @classmethod
    def create(
        cls,
        source: ExecutionSource,
        zones_str: Optional[str] = None,
        execution_id: Optional[str] = None,
    ) -> ExecutionContext:
        """Factory method to construct an ExecutionContext.

        Args:
            source: 'SCHEDULED' or 'MANUAL_API'.
            zones_str: Optional zone selection string (e.g. 'NZ', 'NZ,WZ', 'ALL').
                       If None: falls back to configured `settings.enabled_zones`.
                       Never defaults an omitted manual request to 'ALL' if configuration is restricted.
            execution_id: Optional explicit execution identifier. If omitted, generates a unique ID.

        Returns:
            An immutable ExecutionContext instance.

        Raises:
            ValueError: If source is invalid or execution_id is empty.
            InvalidZoneError: If zone selection is invalid or contradictory.
        """
        if source not in VALID_SOURCES:
            raise ValueError(
                f"Invalid execution source '{source}'. Must be one of: {', '.join(sorted(VALID_SOURCES))}"
            )

        exec_id = execution_id.strip() if execution_id and execution_id.strip() else f"exec_{uuid.uuid4().hex[:16]}"
        effective_zones = zones_str if zones_str is not None else settings.enabled_zones

        selection = resolve_zones(effective_zones)
        return cls(
            execution_id=exec_id,
            source=source,
            selection=selection,
        )

    @classmethod
    def for_scheduled(
        cls,
        execution_id: Optional[str] = None,
    ) -> ExecutionContext:
        """Convenience constructor for scheduled background jobs."""
        return cls.create(
            source="SCHEDULED",
            zones_str=None,
            execution_id=execution_id,
        )

    @classmethod
    def for_manual(
        cls,
        zones_str: Optional[str] = None,
        execution_id: Optional[str] = None,
    ) -> ExecutionContext:
        """Convenience constructor for manual API triggers."""
        return cls.create(
            source="MANUAL_API",
            zones_str=zones_str,
            execution_id=execution_id,
        )


def create_execution_context(
    source: ExecutionSource,
    zones_str: Optional[str] = None,
    execution_id: Optional[str] = None,
) -> ExecutionContext:
    """Convenience functional interface for creating an ExecutionContext."""
    return ExecutionContext.create(
        source=source,
        zones_str=zones_str,
        execution_id=execution_id,
    )
