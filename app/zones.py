"""Zone registry and strict zone resolution for pyro_auto_frc.

Authoritative Zone Mappings:
  NZ (North Zone) = [2, 55, 56, 59, 60, 61, 62, 64, 65]  (9 circles)
  WZ (West Zone)  = [1, 3, 4, 10, 12]                     (5 circles)
  EZ (East Zone)  = [70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 99] (11 circles)
  SZ (South Zone) = [40, 41, 50, 51, 53, 54]             (6 circles)
  ALL             = unrestricted (all circles)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Literal, Optional, Tuple


class InvalidZoneError(ValueError):
    """Raised when an invalid, malformed, or contradictory zone string is provided."""
    pass


# Authoritative circle code lists per zone
NZ: List[int] = [2, 55, 56, 59, 60, 61, 62, 64, 65]
WZ: List[int] = [1, 3, 4, 10, 12]
EZ: List[int] = [70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 99]
SZ: List[int] = [40, 41, 50, 51, 53, 54]

ZONE_MAP: Dict[str, List[int]] = {
    "NZ": NZ,
    "WZ": WZ,
    "EZ": EZ,
    "SZ": SZ,
}

VALID_ZONES: frozenset[str] = frozenset(ZONE_MAP.keys())

# Comprehensive circle metadata derived from cos_circles reference
CIRCLE_METADATA: Dict[int, Dict[str, str]] = {
    # NZ (9 circles)
    2: {"circle_name": "DELHI", "short_code": "DL", "zone_code": "NZ"},
    55: {"circle_name": "HIMACHAL PRADESH", "short_code": "HP", "zone_code": "NZ"},
    56: {"circle_name": "PUNJAB", "short_code": "PB", "zone_code": "NZ"},
    59: {"circle_name": "RAJASTHAN", "short_code": "RJ", "zone_code": "NZ"},
    60: {"circle_name": "UPEAST", "short_code": "UE", "zone_code": "NZ"},
    61: {"circle_name": "HARYANA", "short_code": "HR", "zone_code": "NZ"},
    62: {"circle_name": "UPWEST", "short_code": "UW", "zone_code": "NZ"},
    64: {"circle_name": "UTTARANCHAL", "short_code": "UL", "zone_code": "NZ"},
    65: {"circle_name": "JAMMU AND KASHMIR", "short_code": "JK", "zone_code": "NZ"},
    # WZ (5 circles)
    1: {"circle_name": "MAHARASHTRA", "short_code": "MH", "zone_code": "WZ"},
    3: {"circle_name": "CHHATTISGARH", "short_code": "CG", "zone_code": "WZ"},
    4: {"circle_name": "MADHYA PRADESH", "short_code": "MP", "zone_code": "WZ"},
    10: {"circle_name": "GUJARAT", "short_code": "GJ", "zone_code": "WZ"},
    12: {"circle_name": "MUMBAI", "short_code": "MU", "zone_code": "WZ"},
    # EZ (11 circles)
    70: {"circle_name": "WEST BENGAL TELECOM CIRCLE", "short_code": "WB", "zone_code": "EZ"},
    71: {"circle_name": "ASSAM TELCOM CIRCLE", "short_code": "AS", "zone_code": "EZ"},
    72: {"circle_name": "ODISHA TELECOM CIRCLE", "short_code": "OR", "zone_code": "EZ"},
    73: {"circle_name": "BIHAR TELECOM CIRCLE", "short_code": "BH", "zone_code": "EZ"},
    74: {"circle_name": "NORTH EAST-1 TELECOM CIRCLE", "short_code": "NE1", "zone_code": "EZ"},
    75: {"circle_name": "NORTH EAST-2 TELECOM CIRCLE", "short_code": "NE2", "zone_code": "EZ"},
    76: {"circle_name": "JHARKHAND TELECOM CIRCLE", "short_code": "JH", "zone_code": "EZ"},
    77: {"circle_name": "ANDHAMAN TELECOM CIRCLE", "short_code": "AN", "zone_code": "EZ"},
    78: {"circle_name": "CALCUTTA TELECOM DISTRICT", "short_code": "CTD", "zone_code": "EZ"},
    79: {"circle_name": "SIKKIM TELECOM CIRCLE", "short_code": "SK", "zone_code": "EZ"},
    99: {"circle_name": "sysadmin", "short_code": "CO", "zone_code": "EZ"},
    # SZ (6 circles)
    40: {"circle_name": "CHENNAI", "short_code": "CH", "zone_code": "SZ"},
    41: {"circle_name": "TELANGANA", "short_code": "TS", "zone_code": "SZ"},
    50: {"circle_name": "KERALA", "short_code": "KE", "zone_code": "SZ"},
    51: {"circle_name": "ANDHRA PRADESH", "short_code": "AP", "zone_code": "SZ"},
    53: {"circle_name": "KARNATAKA", "short_code": "KA", "zone_code": "SZ"},
    54: {"circle_name": "TAMIL NADU", "short_code": "TN", "zone_code": "SZ"},
}

# Circle to Zone lookup table
CIRCLE_TO_ZONE: Dict[int, str] = {
    circle_id: meta["zone_code"] for circle_id, meta in CIRCLE_METADATA.items()
}


@dataclass(frozen=True)
class ZoneSelection:
    """Immutable representation of a resolved zone selection.

    Attributes:
        zone_codes: Tuple of uppercase zone code strings, e.g. ('NZ', 'WZ') or ('ALL',).
        circle_codes: Tuple of integer circle IDs if FILTERED mode, or None if ALL mode.
        mode: 'ALL' (unrestricted nationwide) or 'FILTERED' (scoped to circle_codes).
    """

    zone_codes: Tuple[str, ...]
    circle_codes: Optional[Tuple[int, ...]]
    mode: Literal["ALL", "FILTERED"]

    def is_circle_allowed(self, circle_code: int | str) -> bool:
        """Check if a circle code is permitted under this selection."""
        if self.mode == "ALL" or self.circle_codes is None:
            return True
        try:
            return int(circle_code) in self.circle_codes
        except (ValueError, TypeError):
            return False

    def to_dict(self) -> dict:
        """Serialize selection metadata for API responses and logging."""
        return {
            "mode": self.mode,
            "zone_codes": list(self.zone_codes),
            "circle_codes": list(self.circle_codes) if self.circle_codes is not None else None,
            "circle_count": len(self.circle_codes) if self.circle_codes is not None else len(CIRCLE_METADATA),
        }


def get_zone_circles(zone_code: str) -> List[int]:
    """Retrieve the circle codes associated with a specific zone code.

    Args:
        zone_code: Case-insensitive zone identifier (e.g. 'NZ', 'wz').

    Returns:
        A copy of the list of integer circle codes for the specified zone.

    Raises:
        InvalidZoneError: If zone_code is invalid or not found.
    """
    if not isinstance(zone_code, str):
        raise InvalidZoneError(f"Zone code must be a string, got {type(zone_code).__name__}")

    normalized = zone_code.strip().upper()
    if normalized not in ZONE_MAP:
        raise InvalidZoneError(
            f"Unknown zone code '{zone_code}'. Valid zone codes are: {', '.join(sorted(VALID_ZONES))}"
        )
    return list(ZONE_MAP[normalized])


def get_circle_metadata_dict() -> Dict[int, Dict[str, str]]:
    """Retrieve a copy of all circle metadata."""
    return {k: dict(v) for k, v in CIRCLE_METADATA.items()}


def get_circle_zone(circle_code: int | str) -> Optional[str]:
    """Get the zone code for a given circle code, or None if not recognized."""
    try:
        return CIRCLE_TO_ZONE.get(int(circle_code))
    except (ValueError, TypeError):
        return None


def resolve_zones(zones_str: Optional[str]) -> ZoneSelection:
    """Strictly parse and resolve a zone selection string into an immutable ZoneSelection.

    Validation Rules:
        - Must be a non-empty string. Empty string, whitespace, or None raises InvalidZoneError.
        - Whitespace around zone codes is trimmed.
        - Zone codes are case-insensitive ('nz' -> 'NZ').
        - Duplicate zone codes are deduplicated ('NZ,NZ' -> 'NZ').
        - 'ALL' cannot be combined with specific zones ('ALL,NZ' is rejected).
        - Consecutive commas or leading/trailing commas ('NZ,,WZ', ',NZ') are rejected.
        - Unknown zone codes ('NZZ', 'UNKNOWN') are rejected.
        - Never silently defaults malformed input to 'ALL'.

    Args:
        zones_str: Comma-delimited zone string (e.g. 'NZ', 'NZ,WZ', 'ALL').

    Returns:
        ZoneSelection object containing resolved zone_codes, circle_codes, and mode.

    Raises:
        InvalidZoneError: If the input string is invalid, malformed, or contradictory.
    """
    if zones_str is None:
        raise InvalidZoneError("Zone selection cannot be None.")

    if not isinstance(zones_str, str):
        raise InvalidZoneError(f"Zone selection must be a string, got {type(zones_str).__name__}.")

    trimmed = zones_str.strip()
    if not trimmed:
        raise InvalidZoneError("Zone selection cannot be empty.")

    raw_tokens = zones_str.split(",")
    normalized_tokens: List[str] = []

    for token in raw_tokens:
        clean = token.strip()
        if not clean:
            raise InvalidZoneError(
                f"Malformed zone selection '{zones_str}': contains empty zone token."
            )
        normalized_tokens.append(clean.upper())

    # Check for 'ALL'
    if "ALL" in normalized_tokens:
        if len(normalized_tokens) > 1:
            raise InvalidZoneError(
                f"Contradictory zone selection '{zones_str}': 'ALL' cannot be combined with specific zones."
            )
        return ZoneSelection(
            zone_codes=("ALL",),
            circle_codes=None,
            mode="ALL",
        )

    # Validate individual zone codes
    invalid_codes = [code for code in normalized_tokens if code not in VALID_ZONES]
    if invalid_codes:
        valid_options = ", ".join(sorted(VALID_ZONES)) + ", ALL"
        raise InvalidZoneError(
            f"Invalid zone code(s) {invalid_codes} in selection '{zones_str}'. Valid options: {valid_options}"
        )

    # Deduplicate while preserving order of appearance
    deduped_zones = tuple(dict.fromkeys(normalized_tokens))

    # Collect and sort all matching circle codes
    all_circles = set()
    for zone in deduped_zones:
        all_circles.update(ZONE_MAP[zone])

    return ZoneSelection(
        zone_codes=deduped_zones,
        circle_codes=tuple(sorted(all_circles)),
        mode="FILTERED",
    )
