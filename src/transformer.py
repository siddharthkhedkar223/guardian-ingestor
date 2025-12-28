"""
transformer.py - Validates and cleans a raw API record before DB write.
"""

from typing import Dict, Any
from logger import get_logger

log = get_logger("transformer")

REQUIRED_FIELDS: Dict[str, type] = {
    "id": int,
    "userId": int,
    "title": str,
    "body": str,
}


class TransformationError(Exception):
    """Raised when a record fails schema validation."""
    pass


def validate_and_transform(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate required fields and types, then sanitise string values.
    Raises TransformationError if validation fails.
    """
    # Check all required fields are present
    missing = [f for f in REQUIRED_FIELDS if f not in raw]
    if missing:
        raise TransformationError(
            f"Record ID={raw.get('id', '?')} missing fields: {missing}"
        )

    # Check field types
    for field, expected_type in REQUIRED_FIELDS.items():
        if not isinstance(raw[field], expected_type):
            raise TransformationError(
                f"Record ID={raw.get('id')}: '{field}' expected "
                f"{expected_type.__name__}, got {type(raw[field]).__name__}."
            )

    cleaned = {
        "id":     raw["id"],
        "userId": raw["userId"],
        "title":  _clean_string(raw["title"]),
        "body":   _clean_string(raw["body"]),
    }

    log.debug("Transformed Record ID=%d.", cleaned["id"])
    return cleaned


def _clean_string(value: str) -> str:
    return " ".join(value.split())
