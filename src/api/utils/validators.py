from typing import Dict, Any
from .exceptions import ValidationError

REQUIRED_FIELDS = [
    "project_id",
    "gcs_bucket",
    "gcs_folder",
    "job_id",
    "business_id",
    "type",
]

VALID_TYPES = ["provinsi", "kabupaten", "kecamatan", "school"]


def validate_payload(payload: Dict[str, Any]) -> None:
    # Check required fields
    for field in REQUIRED_FIELDS:
        if field not in payload:
            raise ValidationError(f"Missing required field: {field}")

    # Validate data type
    if payload["type"] not in VALID_TYPES:
        raise ValidationError(f"Invalid type. Must be one of: {', '.join(VALID_TYPES)}")

    # Validate job_id format
    try:
        int(str(payload["job_id"]))
    except ValueError:
        raise ValidationError("job_id must be a valid integer")
