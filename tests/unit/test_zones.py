"""Unit tests for app/zones.py (Zone Registry and Strict Resolver)."""

import pytest

from app.zones import (
    CIRCLE_METADATA,
    CIRCLE_TO_ZONE,
    EZ,
    NZ,
    SZ,
    VALID_ZONES,
    WZ,
    ZONE_MAP,
    InvalidZoneError,
    ZoneSelection,
    get_circle_metadata_dict,
    get_circle_zone,
    get_zone_circles,
    resolve_zones,
)


class TestZoneConstants:
    """Validate authoritative zone definitions and metadata."""

    def test_zone_definitions(self):
        assert NZ == [2, 55, 56, 59, 60, 61, 62, 64, 65]
        assert WZ == [1, 3, 4, 10, 12]
        assert EZ == [70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 99]
        assert SZ == [40, 41, 50, 51, 53, 54]

        assert len(NZ) == 9
        assert len(WZ) == 5
        assert len(EZ) == 11
        assert len(SZ) == 6
        assert len(CIRCLE_METADATA) == 31

    def test_valid_zones_set(self):
        assert VALID_ZONES == frozenset({"NZ", "WZ", "EZ", "SZ"})
        assert set(ZONE_MAP.keys()) == set(VALID_ZONES)

    def test_no_circle_overlaps(self):
        """Ensure no circle ID is assigned to multiple zones."""
        all_circles = NZ + WZ + EZ + SZ
        assert len(all_circles) == len(set(all_circles)), "Duplicate circle ID found across zones!"

    def test_circle_to_zone_lookup(self):
        assert get_circle_zone(2) == "NZ"
        assert get_circle_zone("2") == "NZ"
        assert get_circle_zone(12) == "WZ"
        assert get_circle_zone(70) == "EZ"
        assert get_circle_zone(40) == "SZ"
        assert get_circle_zone(9999) is None
        assert get_circle_zone("invalid") is None

    def test_circle_metadata_integrity(self):
        metadata = get_circle_metadata_dict()
        assert len(metadata) == 31
        assert metadata[2]["circle_name"] == "DELHI"
        assert metadata[2]["short_code"] == "DL"
        assert metadata[2]["zone_code"] == "NZ"

        # Ensure returned dict is a copy and mutating it doesn't affect original
        metadata[2]["circle_name"] = "MODIFIED"
        assert CIRCLE_METADATA[2]["circle_name"] == "DELHI"


class TestGetZoneCircles:
    """Validate get_zone_circles helper function."""

    def test_get_valid_zones(self):
        assert get_zone_circles("NZ") == [2, 55, 56, 59, 60, 61, 62, 64, 65]
        assert get_zone_circles("wz") == [1, 3, 4, 10, 12]
        assert get_zone_circles(" EZ ") == [70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 99]
        assert get_zone_circles("sz") == [40, 41, 50, 51, 53, 54]

    def test_get_zone_circles_returns_independent_copy(self):
        circles = get_zone_circles("NZ")
        circles.append(9999)
        assert 9999 not in ZONE_MAP["NZ"]

    def test_get_invalid_zones(self):
        with pytest.raises(InvalidZoneError, match="Unknown zone code 'ALL'"):
            get_zone_circles("ALL")

        with pytest.raises(InvalidZoneError, match="Unknown zone code 'NZZ'"):
            get_zone_circles("NZZ")

        with pytest.raises(InvalidZoneError, match="Unknown zone code 'UNKNOWN'"):
            get_zone_circles("UNKNOWN")

        with pytest.raises(InvalidZoneError, match="Zone code must be a string"):
            get_zone_circles(123)  # type: ignore


class TestResolveZonesAllowed:
    """Validate all permitted zone resolution scenarios."""

    def test_resolve_nz_uppercase(self):
        selection = resolve_zones("NZ")
        assert selection.mode == "FILTERED"
        assert selection.zone_codes == ("NZ",)
        assert selection.circle_codes == (2, 55, 56, 59, 60, 61, 62, 64, 65)
        assert len(selection.circle_codes) == 9

    def test_resolve_wz(self):
        selection = resolve_zones("WZ")
        assert selection.mode == "FILTERED"
        assert selection.zone_codes == ("WZ",)
        assert selection.circle_codes == (1, 3, 4, 10, 12)

    def test_resolve_ez(self):
        selection = resolve_zones("EZ")
        assert selection.mode == "FILTERED"
        assert selection.zone_codes == ("EZ",)
        assert selection.circle_codes == (70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 99)

    def test_resolve_sz(self):
        selection = resolve_zones("SZ")
        assert selection.mode == "FILTERED"
        assert selection.zone_codes == ("SZ",)
        assert selection.circle_codes == (40, 41, 50, 51, 53, 54)

    def test_resolve_case_insensitive_lowercase(self):
        selection = resolve_zones("nz")
        assert selection.mode == "FILTERED"
        assert selection.zone_codes == ("NZ",)
        assert selection.circle_codes == (2, 55, 56, 59, 60, 61, 62, 64, 65)

    def test_resolve_whitespace_padded(self):
        selection = resolve_zones(" NZ ")
        assert selection.mode == "FILTERED"
        assert selection.zone_codes == ("NZ",)
        assert selection.circle_codes == (2, 55, 56, 59, 60, 61, 62, 64, 65)

    def test_resolve_multi_zone_nz_wz(self):
        selection = resolve_zones("NZ,WZ")
        assert selection.mode == "FILTERED"
        assert selection.zone_codes == ("NZ", "WZ")
        expected_circles = tuple(sorted(NZ + WZ))
        assert selection.circle_codes == expected_circles
        assert len(selection.circle_codes) == 14

    def test_resolve_multi_zone_with_whitespace(self):
        selection = resolve_zones(" nz ,  wz ")
        assert selection.mode == "FILTERED"
        assert selection.zone_codes == ("NZ", "WZ")
        assert selection.circle_codes == tuple(sorted(NZ + WZ))

    def test_resolve_deduplication(self):
        selection = resolve_zones("NZ,NZ")
        assert selection.mode == "FILTERED"
        assert selection.zone_codes == ("NZ",)
        assert selection.circle_codes == (2, 55, 56, 59, 60, 61, 62, 64, 65)

    def test_resolve_multi_zone_order_preserved(self):
        selection = resolve_zones("WZ,NZ")
        assert selection.mode == "FILTERED"
        assert selection.zone_codes == ("WZ", "NZ")
        # circle_codes are always consistently sorted
        assert selection.circle_codes == tuple(sorted(WZ + NZ))

    def test_resolve_all_zones(self):
        selection = resolve_zones("ALL")
        assert selection.mode == "ALL"
        assert selection.zone_codes == ("ALL",)
        assert selection.circle_codes is None

    def test_resolve_all_case_insensitive(self):
        selection = resolve_zones("all")
        assert selection.mode == "ALL"
        assert selection.zone_codes == ("ALL",)
        assert selection.circle_codes is None

    def test_resolve_all_whitespace(self):
        selection = resolve_zones(" ALL ")
        assert selection.mode == "ALL"
        assert selection.zone_codes == ("ALL",)
        assert selection.circle_codes is None

    def test_resolve_all_four_zones(self):
        selection = resolve_zones("NZ,WZ,EZ,SZ")
        assert selection.mode == "FILTERED"
        assert selection.zone_codes == ("NZ", "WZ", "EZ", "SZ")
        assert len(selection.circle_codes) == 31


class TestResolveZonesRejected:
    """Validate all rejection and validation failure scenarios."""

    def test_reject_none(self):
        with pytest.raises(InvalidZoneError, match="Zone selection cannot be None"):
            resolve_zones(None)

    def test_reject_empty_string(self):
        with pytest.raises(InvalidZoneError, match="Zone selection cannot be empty"):
            resolve_zones("")

    def test_reject_whitespace_only(self):
        with pytest.raises(InvalidZoneError, match="Zone selection cannot be empty"):
            resolve_zones("   ")

    def test_reject_non_string(self):
        with pytest.raises(InvalidZoneError, match="Zone selection must be a string"):
            resolve_zones(123)  # type: ignore

    def test_reject_invalid_code_nzz(self):
        with pytest.raises(InvalidZoneError, match="Invalid zone code.*NZZ"):
            resolve_zones("NZZ")

    def test_reject_unknown(self):
        with pytest.raises(InvalidZoneError, match="Invalid zone code.*UNKNOWN"):
            resolve_zones("UNKNOWN")

    def test_reject_empty_token_consecutive_commas(self):
        with pytest.raises(InvalidZoneError, match="contains empty zone token"):
            resolve_zones("NZ,,WZ")

    def test_reject_leading_comma(self):
        with pytest.raises(InvalidZoneError, match="contains empty zone token"):
            resolve_zones(",NZ")

    def test_reject_trailing_comma(self):
        with pytest.raises(InvalidZoneError, match="contains empty zone token"):
            resolve_zones("NZ,")

    def test_reject_multiple_commas_only(self):
        with pytest.raises(InvalidZoneError, match="contains empty zone token"):
            resolve_zones(",,")

    def test_reject_all_combined_with_specific_zone_prefix(self):
        with pytest.raises(InvalidZoneError, match="'ALL' cannot be combined with specific zones"):
            resolve_zones("ALL,NZ")

    def test_reject_all_combined_with_specific_zone_suffix(self):
        with pytest.raises(InvalidZoneError, match="'ALL' cannot be combined with specific zones"):
            resolve_zones("NZ,ALL")

    def test_reject_partial_unknown(self):
        with pytest.raises(InvalidZoneError, match="Invalid zone code.*UNKNOWN"):
            resolve_zones("NZ,UNKNOWN")

    def test_no_silent_fallback_to_all(self):
        """Assert that invalid inputs never silently return mode='ALL'."""
        invalid_inputs = ["", "NZZ", "UNKNOWN", "NZ,,WZ", "ALL,NZ", "NZ,UNKNOWN"]
        for inp in invalid_inputs:
            with pytest.raises(InvalidZoneError):
                resolve_zones(inp)


class TestZoneSelectionBehavior:
    """Validate ZoneSelection methods and immutability."""

    def test_is_circle_allowed_in_filtered_mode(self):
        nz_selection = resolve_zones("NZ")
        # In NZ
        assert nz_selection.is_circle_allowed(2) is True
        assert nz_selection.is_circle_allowed("2") is True
        assert nz_selection.is_circle_allowed(55) is True
        assert nz_selection.is_circle_allowed("65") is True

        # In WZ (not allowed in NZ)
        assert nz_selection.is_circle_allowed(1) is False
        assert nz_selection.is_circle_allowed(12) is False
        assert nz_selection.is_circle_allowed("12") is False

        # Non-numeric / invalid
        assert nz_selection.is_circle_allowed("invalid") is False

    def test_is_circle_allowed_in_all_mode(self):
        all_selection = resolve_zones("ALL")
        assert all_selection.is_circle_allowed(2) is True
        assert all_selection.is_circle_allowed(12) is True
        assert all_selection.is_circle_allowed(70) is True
        assert all_selection.is_circle_allowed(40) is True
        assert all_selection.is_circle_allowed(9999) is True

    def test_immutability(self):
        selection = resolve_zones("NZ")
        with pytest.raises((AttributeError, TypeError)):
            selection.mode = "ALL"  # type: ignore

        with pytest.raises((AttributeError, TypeError)):
            selection.zone_codes = ("ALL",)  # type: ignore

    def test_to_dict_serialization(self):
        filtered = resolve_zones("NZ,WZ")
        d_filtered = filtered.to_dict()
        assert d_filtered["mode"] == "FILTERED"
        assert d_filtered["zone_codes"] == ["NZ", "WZ"]
        assert len(d_filtered["circle_codes"]) == 14
        assert d_filtered["circle_count"] == 14

        all_sel = resolve_zones("ALL")
        d_all = all_sel.to_dict()
        assert d_all["mode"] == "ALL"
        assert d_all["zone_codes"] == ["ALL"]
        assert d_all["circle_codes"] is None
        assert d_all["circle_count"] == 31
