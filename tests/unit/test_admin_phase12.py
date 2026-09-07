"""Unit tests for Phase 12: Admin API endpoints and zone query overrides.

Verifies:
1. GET /admin/zones:
   - Security: requires X-Admin-Api-Key.
   - Exposes active configuration, resolved mode, active zone codes, active circle count, active circles, and all available zones.
   - Correctly reflects settings.enabled_zones (FILTERED vs ALL).
2. POST /admin/trigger-batch-population:
   - Security: requires X-Admin-Api-Key.
   - Default when zones omitted: uses settings.enabled_zones (never defaults to ALL if restricted).
   - Zone override: applies supplied ?zones=... parameter without mutating settings.enabled_zones.
   - Response metadata: includes execution_id, execution_source, effective_zones, mode, circles_count, summary.
   - Invalid zone string: returns HTTP 400 with error details.
   - Lock contention (busy advisory lock): returns HTTP 409 with execution metadata.
3. POST /admin/trigger-recharge:
   - Security: requires X-Admin-Api-Key.
   - Default when zones omitted: uses settings.enabled_zones.
   - Zone override: applies supplied ?zones=... parameter without mutating settings.enabled_zones.
   - Response metadata: includes execution_id, execution_source, effective_zones, mode, circles_count, summary.
   - Invalid zone string: returns HTTP 400 with error details.
"""

from unittest.mock import AsyncMock, patch
import pytest
from fastapi.testclient import TestClient

from app.config import settings
from app.context import ExecutionContext
from main import app


class TestAdminEndpointsAuth:
    def setup_method(self):
        self.client = TestClient(app)

    def test_get_zones_missing_api_key(self):
        """GET /admin/zones fails without admin API key."""
        response = self.client.get("/admin/zones")
        assert response.status_code in (401, 403)

    def test_trigger_batch_missing_api_key(self):
        """POST /admin/trigger-batch-population fails without admin API key."""
        response = self.client.post("/admin/trigger-batch-population")
        assert response.status_code in (401, 403)

    def test_trigger_recharge_missing_api_key(self):
        """POST /admin/trigger-recharge fails without admin API key."""
        response = self.client.post("/admin/trigger-recharge")
        assert response.status_code in (401, 403)


class TestAdminZonesEndpoint:
    def setup_method(self):
        self.client = TestClient(app)
        self.headers = {"X-Admin-Api-Key": settings.admin_api_key}

    def test_get_zones_filtered_configuration(self):
        """Returns metadata reflecting FILTERED zone configuration."""
        with patch.object(settings, "enabled_zones", "NZ"):
            response = self.client.get("/admin/zones", headers=self.headers)

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "HEALTHY"
        assert body["configured_zones"] == "NZ"
        assert body["resolved_mode"] == "FILTERED"
        assert body["active_zone_codes"] == ["NZ"]
        assert body["active_circle_count"] == 9
        assert body["active_circles"] == [2, 55, 56, 59, 60, 61, 62, 64, 65]
        assert body["all_available_zones"] == ["EZ", "NZ", "SZ", "WZ"]

    def test_get_zones_all_configuration(self):
        """Returns metadata reflecting ALL zone configuration."""
        with patch.object(settings, "enabled_zones", "ALL"):
            response = self.client.get("/admin/zones", headers=self.headers)

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "HEALTHY"
        assert body["configured_zones"] == "ALL"
        assert body["resolved_mode"] == "ALL"
        assert body["active_zone_codes"] == ["ALL"]
        assert body["active_circle_count"] == 31
        assert len(body["active_circles"]) == 31


class TestTriggerBatchPopulationEndpoint:
    def setup_method(self):
        self.client = TestClient(app)
        self.headers = {"X-Admin-Api-Key": settings.admin_api_key}

    @patch("app.batch.populator.run_batch_population")
    def test_trigger_batch_omitted_zones_uses_settings(self, mock_populator):
        """Omitted zones parameter uses settings.enabled_zones without defaulting to ALL."""
        mock_populator.return_value = {"inserted": 1, "dispatchable": 1}

        with patch.object(settings, "enabled_zones", "NZ"):
            response = self.client.post(
                "/admin/trigger-batch-population",
                headers=self.headers,
            )

        assert response.status_code == 200
        body = response.json()
        assert body["triggered"] is True
        assert body["execution_source"] == "MANUAL_API"
        assert body["effective_zones"] == ["NZ"]
        assert body["mode"] == "FILTERED"
        assert body["circles_count"] == 9
        assert "execution_id" in body
        assert body["summary"]["inserted"] == 1

        # Check context passed to populator
        passed_ctx = mock_populator.call_args.kwargs.get("context")
        assert passed_ctx.zone_codes == ("NZ",)
        assert passed_ctx.mode == "FILTERED"

    @patch("app.batch.populator.run_batch_population")
    def test_trigger_batch_explicit_zones_override(self, mock_populator):
        """Supplied zones parameter overrides configuration without altering settings."""
        mock_populator.return_value = {"inserted": 2, "dispatchable": 2}

        with patch.object(settings, "enabled_zones", "NZ"):
            response = self.client.post(
                "/admin/trigger-batch-population?zones=WZ",
                headers=self.headers,
            )
            # Configuration was not mutated
            assert settings.enabled_zones == "NZ"

        assert response.status_code == 200
        body = response.json()
        assert body["effective_zones"] == ["WZ"]
        assert body["mode"] == "FILTERED"
        assert body["circles_count"] == 5

        passed_ctx = mock_populator.call_args.kwargs.get("context")
        assert passed_ctx.zone_codes == ("WZ",)

    def test_trigger_batch_invalid_zone_returns_400(self):
        """Invalid zone string returns HTTP 400 Bad Request."""
        response = self.client.post(
            "/admin/trigger-batch-population?zones=INVALID_ZONE",
            headers=self.headers,
        )
        assert response.status_code == 400
        body = response.json()
        assert "detail" in body

    def test_trigger_batch_contradictory_zones_returns_400(self):
        """Contradictory zone string (ALL,NZ) returns HTTP 400 Bad Request."""
        response = self.client.post(
            "/admin/trigger-batch-population?zones=ALL,NZ",
            headers=self.headers,
        )
        assert response.status_code == 400
        body = response.json()
        assert "detail" in body

    @patch("app.batch.populator.run_batch_population")
    def test_trigger_batch_lock_contention_returns_409_with_metadata(self, mock_populator):
        """When lock is busy, returns HTTP 409 Conflict with full metadata."""
        mock_populator.return_value = {
            "status": "SKIPPED_LOCK_BUSY",
            "skipped_lock_busy": True,
            "inserted": 0,
        }

        with patch.object(settings, "enabled_zones", "NZ"):
            response = self.client.post(
                "/admin/trigger-batch-population",
                headers=self.headers,
            )

        assert response.status_code == 409
        body = response.json()
        assert body["triggered"] is False
        assert "already in progress" in body["reason"]
        assert body["execution_source"] == "MANUAL_API"
        assert body["effective_zones"] == ["NZ"]
        assert body["mode"] == "FILTERED"
        assert body["circles_count"] == 9
        assert "execution_id" in body
        assert body["summary"]["skipped_lock_busy"] is True


class TestTriggerRechargeEndpoint:
    def setup_method(self):
        self.client = TestClient(app)
        self.headers = {"X-Admin-Api-Key": settings.admin_api_key}

    @patch("app.processor.process_pending_recharges", new_callable=AsyncMock)
    def test_trigger_recharge_omitted_zones_uses_settings(self, mock_processor):
        """Omitted zones parameter uses settings.enabled_zones."""
        mock_processor.return_value = {"processed": 4, "registered": 4}

        with patch.object(settings, "enabled_zones", "NZ"):
            response = self.client.post(
                "/admin/trigger-recharge",
                headers=self.headers,
            )

        assert response.status_code == 200
        body = response.json()
        assert body["triggered"] is True
        assert body["execution_source"] == "MANUAL_API"
        assert body["effective_zones"] == ["NZ"]
        assert body["mode"] == "FILTERED"
        assert body["circles_count"] == 9
        assert "execution_id" in body
        assert body["summary"]["processed"] == 4

        passed_ctx = mock_processor.call_args.kwargs.get("context")
        assert passed_ctx.zone_codes == ("NZ",)

    @patch("app.processor.process_pending_recharges", new_callable=AsyncMock)
    def test_trigger_recharge_explicit_zones_override(self, mock_processor):
        """Explicit zones parameter overrides configuration."""
        mock_processor.return_value = {"processed": 1}

        with patch.object(settings, "enabled_zones", "NZ"):
            response = self.client.post(
                "/admin/trigger-recharge?zones=NZ,WZ",
                headers=self.headers,
            )
            # Ensure settings was not modified
            assert settings.enabled_zones == "NZ"

        assert response.status_code == 200
        body = response.json()
        assert body["effective_zones"] == ["NZ", "WZ"]
        assert body["mode"] == "FILTERED"
        assert body["circles_count"] == 14

        passed_ctx = mock_processor.call_args.kwargs.get("context")
        assert passed_ctx.zone_codes == ("NZ", "WZ")

    def test_trigger_recharge_invalid_zone_returns_400(self):
        """Invalid zone parameter returns HTTP 400 Bad Request."""
        response = self.client.post(
            "/admin/trigger-recharge?zones=UNKNOWN",
            headers=self.headers,
        )
        assert response.status_code == 400
        body = response.json()
        assert "detail" in body
