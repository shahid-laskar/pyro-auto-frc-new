"""Unit tests for Settings.enabled_zones configuration and Pydantic validation (Phase 2)."""

import pytest
from pydantic import ValidationError

from app.config import Settings
from app.zones import ZoneSelection

# Minimal required fields for Settings instantiation in tests
DUMMY_SETTINGS_KWARGS = {
    "pyro_base_url": "https://dummy-pyro.example.com",
    "pyro_api_key": "dummy_key",
    "pyro_login_id": "dummy_login",
    "pyro_password": "dummy_password",
    "pyro_secret_key": "dummy_secret_key_24b",
    "oracle_user": "dummy_user",
    "oracle_password": "dummy_password",
    "oracle_dsn": "dummy_host:1521/dummy_service",
    "pg_host": "localhost",
    "pg_database": "dummy_db",
    "pg_user": "dummy_user",
    "pg_password": "dummy_password",
    "callback_base_url": "https://callback.example.com",
}


class TestSettingsEnabledZones:
    """Validate Settings.enabled_zones default and validation behaviors."""

    def test_default_enabled_zones_is_all(self):
        settings = Settings(**DUMMY_SETTINGS_KWARGS)
        assert settings.enabled_zones == "ALL"
        assert isinstance(settings.zone_selection, ZoneSelection)
        assert settings.zone_selection.mode == "ALL"
        assert settings.zone_selection.circle_codes is None

    def test_no_enabled_circles_field_exists(self):
        """Plan explicitly forbids adding 'enabled_circles' to configuration."""
        settings = Settings(**DUMMY_SETTINGS_KWARGS)
        assert not hasattr(settings, "enabled_circles")
        assert "enabled_circles" not in Settings.model_fields

    def test_valid_single_zone_nz(self):
        settings = Settings(**DUMMY_SETTINGS_KWARGS, enabled_zones="NZ")
        assert settings.enabled_zones == "NZ"
        assert settings.zone_selection.mode == "FILTERED"
        assert settings.zone_selection.zone_codes == ("NZ",)
        assert len(settings.zone_selection.circle_codes) == 9

    def test_valid_lowercase_zone(self):
        settings = Settings(**DUMMY_SETTINGS_KWARGS, enabled_zones="nz")
        assert settings.enabled_zones == "NZ"
        assert settings.zone_selection.mode == "FILTERED"

    def test_valid_whitespace_padded_zone(self):
        settings = Settings(**DUMMY_SETTINGS_KWARGS, enabled_zones="  WZ  ")
        assert settings.enabled_zones == "WZ"
        assert settings.zone_selection.mode == "FILTERED"
        assert settings.zone_selection.zone_codes == ("WZ",)

    def test_valid_multi_zone(self):
        settings = Settings(**DUMMY_SETTINGS_KWARGS, enabled_zones="NZ,WZ")
        assert settings.enabled_zones == "NZ,WZ"
        assert settings.zone_selection.mode == "FILTERED"
        assert settings.zone_selection.zone_codes == ("NZ", "WZ")
        assert len(settings.zone_selection.circle_codes) == 14

    def test_valid_multi_zone_with_whitespace(self):
        settings = Settings(**DUMMY_SETTINGS_KWARGS, enabled_zones="  NZ , WZ  ")
        assert settings.enabled_zones == "NZ,WZ"
        assert settings.zone_selection.zone_codes == ("NZ", "WZ")

    def test_valid_deduplication(self):
        settings = Settings(**DUMMY_SETTINGS_KWARGS, enabled_zones="NZ,NZ")
        assert settings.enabled_zones == "NZ"
        assert settings.zone_selection.zone_codes == ("NZ",)

    def test_valid_explicit_all(self):
        settings = Settings(**DUMMY_SETTINGS_KWARGS, enabled_zones="ALL")
        assert settings.enabled_zones == "ALL"
        assert settings.zone_selection.mode == "ALL"
        assert settings.zone_selection.circle_codes is None

    def test_valid_explicit_all_lowercase(self):
        settings = Settings(**DUMMY_SETTINGS_KWARGS, enabled_zones="all")
        assert settings.enabled_zones == "ALL"
        assert settings.zone_selection.mode == "ALL"

    def test_environment_variable_override(self, monkeypatch):
        monkeypatch.setenv("ENABLED_ZONES", "NZ,WZ")
        settings = Settings(**DUMMY_SETTINGS_KWARGS)
        assert settings.enabled_zones == "NZ,WZ"
        assert settings.zone_selection.zone_codes == ("NZ", "WZ")


class TestSettingsEnabledZonesRejections:
    """Validate that invalid production configuration fails closed (raises ValidationError)."""

    def test_reject_empty_string(self):
        with pytest.raises(ValidationError) as exc_info:
            Settings(**DUMMY_SETTINGS_KWARGS, enabled_zones="")
        assert "Zone selection cannot be empty" in str(exc_info.value)

    def test_reject_whitespace_only(self):
        with pytest.raises(ValidationError) as exc_info:
            Settings(**DUMMY_SETTINGS_KWARGS, enabled_zones="   ")
        assert "Zone selection cannot be empty" in str(exc_info.value)

    def test_reject_invalid_code_nzz(self):
        with pytest.raises(ValidationError) as exc_info:
            Settings(**DUMMY_SETTINGS_KWARGS, enabled_zones="NZZ")
        assert "Invalid zone code" in str(exc_info.value)
        assert "NZZ" in str(exc_info.value)

    def test_reject_unknown(self):
        with pytest.raises(ValidationError) as exc_info:
            Settings(**DUMMY_SETTINGS_KWARGS, enabled_zones="UNKNOWN")
        assert "Invalid zone code" in str(exc_info.value)

    def test_reject_empty_token_consecutive_commas(self):
        with pytest.raises(ValidationError) as exc_info:
            Settings(**DUMMY_SETTINGS_KWARGS, enabled_zones="NZ,,WZ")
        assert "contains empty zone token" in str(exc_info.value)

    def test_reject_all_combined_with_specific_zone(self):
        with pytest.raises(ValidationError) as exc_info:
            Settings(**DUMMY_SETTINGS_KWARGS, enabled_zones="ALL,NZ")
        assert "'ALL' cannot be combined with specific zones" in str(exc_info.value)

    def test_reject_partial_unknown(self):
        with pytest.raises(ValidationError) as exc_info:
            Settings(**DUMMY_SETTINGS_KWARGS, enabled_zones="NZ,UNKNOWN")
        assert "Invalid zone code" in str(exc_info.value)

    def test_env_var_invalid_fails_closed(self, monkeypatch):
        """Ensure invalid environment variable stops settings initialization."""
        monkeypatch.setenv("ENABLED_ZONES", "INVALID_ZONE")
        with pytest.raises(ValidationError) as exc_info:
            Settings(**DUMMY_SETTINGS_KWARGS)
        assert "Invalid zone code" in str(exc_info.value)

    def test_no_silent_fallback_to_all(self):
        """Verify that invalid inputs never silently default to ALL."""
        invalid_inputs = ["", "   ", "NZZ", "UNKNOWN", "NZ,,WZ", "ALL,NZ", "NZ,UNKNOWN"]
        for inp in invalid_inputs:
            with pytest.raises(ValidationError):
                Settings(**DUMMY_SETTINGS_KWARGS, enabled_zones=inp)
