from typing import Optional


def generate_filename(
    data_type: str, data_id: Optional[str], district_cd: Optional[str], job_id: int
) -> str:
    """Generate filename for the CSV file based on the data type and IDs"""
    base_name = f"list_{data_type}"

    if district_cd:
        base_name += f"_{district_cd}"
    elif data_id:
        base_name += f"_{data_id}"

    return f"{base_name}_{job_id}.csv"
