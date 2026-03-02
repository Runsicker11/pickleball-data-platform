"""
Helper utilities for Amazon Ads pipeline — row normalization and type coercion.
"""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Columns that should be coerced to float
FLOAT_COLUMNS = {
    "cost",
    "sales14d",
    "sales",
    "salesClicks",
    "unitsSoldClicks",
    "clickThroughRate",
    "costPerClick",
}

# Columns that should be coerced to int
INT_COLUMNS = {
    "impressions",
    "clicks",
    "purchases14d",
    "purchases",
    "purchasesClicks",
    "unitsSoldClicks14d",
    "unitsSold",
}


def normalize_row(row: dict, profile_id: str) -> dict:
    """Normalize a single report row.

    - Adds profile_id and _loaded_at metadata
    - Coerces numeric types
    - Passes through all other fields unchanged (dlt handles schema evolution)
    """
    row["profile_id"] = profile_id
    row["_loaded_at"] = datetime.utcnow().isoformat()

    for key, value in row.items():
        if value is None:
            continue
        if key in FLOAT_COLUMNS:
            try:
                row[key] = float(value)
            except (ValueError, TypeError):
                row[key] = None
        elif key in INT_COLUMNS:
            try:
                row[key] = int(float(value))  # float() first handles "3.0" strings
            except (ValueError, TypeError):
                row[key] = None

    return row
