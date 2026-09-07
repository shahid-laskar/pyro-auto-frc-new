"""Unit tests for app/context.py (ExecutionContext abstraction - Phase 3)."""

import pytest

from app.config import settings
from app.context import (
    ExecutionContext,
    create_execution_context,
)
from app.zones import (
    CIRCLE_METADATA,
    InvalidZoneError,
    ZoneSelection,
    resolve_zones,
)


class TestExecutionContextDirectInstantiation:
    """Validate direct construction and validation invariants of ExecutionContext."""

    def test_valid_instantiation(self):
        selection = resolve_zones("NZ")
        context = ExecutionContext(
            execution_id="exec_12345",
            source="SCHEDULED",
            selection=selection,
        )
        assert context.execution_id == "exec_12345"
        assert context.source == "SCHEDULED"
        assert context.selection == selection
        assert context.mode == "FILTERED"
        assert context.zone_codes == ("NZ",)
        assert context.circle_codes == (2, 55, 56, 59, 60, 61, 62, 64, 65)
        assert context.circle_count == 9

    def test_immutability(self):
        selection = resolve_zones("ALL")
        context = ExecutionContext(
            execution_id="exec_immutable",
            source="MANUAL_API",
            selection=selection,
        )
        with pytest.raises((AttributeError, TypeError)):
            context.execution_id = "exec_new"  # type: ignore

        with pytest.raises((AttributeError, TypeError)):
            context.source = "SCHEDULED"  # type: ignore

        with pytest.raises((AttributeError, TypeError)):
            context.selection = resolve_zones("NZ")  # type: ignore

    def test_reject_empty_execution_id(self):
        selection = resolve_zones("NZ")
        with pytest.raises(ValueError, match="execution_id must be a non-empty string"):
            ExecutionContext(execution_id="", source="SCHEDULED", selection=selection)

        with pytest.raises(ValueError, match="execution_id must be a non-empty string"):
            ExecutionContext(execution_id="   ", source="SCHEDULED", selection=selection)

    def test_reject_invalid_source(self):
        selection = resolve_zones("NZ")
        with pytest.raises(ValueError, match="Invalid execution source 'INVALID_SOURCE'"):
            ExecutionContext(
                execution_id="exec_1",
                source="INVALID_SOURCE",  # type: ignore
                selection=selection,
            )

    def test_reject_invalid_selection_type(self):
        with pytest.raises(TypeError, match="selection must be an instance of ZoneSelection"):
            ExecutionContext(
                execution_id="exec_1",
                source="SCHEDULED",
                selection="NZ",  # type: ignore
            )


class TestExecutionContextFactoryMethods:
    """Validate create, for_scheduled, and for_manual factory constructors."""

    def test_for_scheduled_inherits_settings(self, monkeypatch):
        monkeypatch.setattr(settings, "enabled_zones", "NZ,WZ")
        context = ExecutionContext.for_scheduled()

        assert context.source == "SCHEDULED"
        assert context.mode == "FILTERED"
        assert context.zone_codes == ("NZ", "WZ")
        assert context.circle_count == 14
        assert context.execution_id.startswith("exec_")

    def test_for_scheduled_explicit_execution_id(self):
        context = ExecutionContext.for_scheduled(execution_id="custom_sched_id")
        assert context.execution_id == "custom_sched_id"
        assert context.source == "SCHEDULED"

    def test_for_manual_with_explicit_zone(self):
        context = ExecutionContext.for_manual(zones_str="WZ")
        assert context.source == "MANUAL_API"
        assert context.mode == "FILTERED"
        assert context.zone_codes == ("WZ",)
        assert context.circle_codes == (1, 3, 4, 10, 12)
        assert context.circle_count == 5

    def test_for_manual_omitted_zone_inherits_settings(self, monkeypatch):
        """When zones is omitted in manual trigger, must use settings without defaulting to ALL."""
        monkeypatch.setattr(settings, "enabled_zones", "NZ")
        context = ExecutionContext.for_manual(zones_str=None)
        assert context.source == "MANUAL_API"
        assert context.mode == "FILTERED"
        assert context.zone_codes == ("NZ",)
        assert context.circle_count == 9

    def test_manual_execution_does_not_mutate_settings(self, monkeypatch):
        """Manual execution with override must never alter application settings."""
        monkeypatch.setattr(settings, "enabled_zones", "ALL")
        assert settings.enabled_zones == "ALL"

        context = ExecutionContext.for_manual(zones_str="NZ")
        assert context.zone_codes == ("NZ",)
        # Settings remain unaltered
        assert settings.enabled_zones == "ALL"

    def test_create_generates_unique_ids(self):
        ctx1 = ExecutionContext.create(source="SCHEDULED")
        ctx2 = ExecutionContext.create(source="SCHEDULED")
        assert ctx1.execution_id != ctx2.execution_id
        assert len(ctx1.execution_id) > 10

    def test_create_with_invalid_zone_fails_closed(self):
        with pytest.raises(InvalidZoneError, match="Invalid zone code.*NZZ"):
            ExecutionContext.create(source="MANUAL_API", zones_str="NZZ")

        with pytest.raises(InvalidZoneError, match="'ALL' cannot be combined with specific zones"):
            ExecutionContext.create(source="MANUAL_API", zones_str="ALL,NZ")

    def test_create_with_invalid_source_raises(self):
        with pytest.raises(ValueError, match="Invalid execution source 'WORKER'"):
            ExecutionContext.create(source="WORKER")  # type: ignore

    def test_functional_create_helper(self):
        ctx = create_execution_context(source="MANUAL_API", zones_str="SZ")
        assert ctx.source == "MANUAL_API"
        assert ctx.zone_codes == ("SZ",)
        assert ctx.circle_codes == (40, 41, 50, 51, 53, 54)


class TestExecutionContextBehavior:
    """Validate circle allowance checking and serialization."""

    def test_is_circle_allowed_filtered(self):
        ctx = ExecutionContext.for_manual(zones_str="NZ")
        # Allowed in NZ
        assert ctx.is_circle_allowed(2) is True
        assert ctx.is_circle_allowed("65") is True

        # Not allowed in NZ (in WZ)
        assert ctx.is_circle_allowed(1) is False
        assert ctx.is_circle_allowed(12) is False

    def test_is_circle_allowed_all(self):
        ctx = ExecutionContext.for_manual(zones_str="ALL")
        assert ctx.is_circle_allowed(2) is True
        assert ctx.is_circle_allowed(1) is True
        assert ctx.is_circle_allowed(70) is True
        assert ctx.is_circle_allowed(40) is True
        assert ctx.is_circle_allowed(9999) is True

    def test_all_mode_circle_count(self):
        ctx = ExecutionContext.for_manual(zones_str="ALL")
        assert ctx.circle_count == len(CIRCLE_METADATA)
        assert ctx.circle_count == 31

    def test_to_dict_filtered(self):
        ctx = ExecutionContext.create(
            source="MANUAL_API",
            zones_str="NZ,WZ",
            execution_id="exec_test_dict",
        )
        d = ctx.to_dict()
        assert d["execution_id"] == "exec_test_dict"
        assert d["source"] == "MANUAL_API"
        assert d["mode"] == "FILTERED"
        assert d["effective_zones"] == ["NZ", "WZ"]
        assert len(d["circle_codes"]) == 14
        assert d["circles_count"] == 14

    def test_to_dict_all(self):
        ctx = ExecutionContext.create(
            source="SCHEDULED",
            zones_str="ALL",
            execution_id="exec_test_all",
        )
        d = ctx.to_dict()
        assert d["execution_id"] == "exec_test_all"
        assert d["source"] == "SCHEDULED"
        assert d["mode"] == "ALL"
        assert d["effective_zones"] == ["ALL"]
        assert d["circle_codes"] is None
        assert d["circles_count"] == 31
