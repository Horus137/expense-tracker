from datetime import datetime, timezone

ALLOWED_CATEGORIES = {
    "food",
    "rent",
    "transport",
    "shopping",
    "entertainment",
    "health",
}

def validate_expense(row: dict):
    """
    Validates a single expense row.
    Returns (is_valid: bool, error_reason: str | None)
    """

    try:
        amount = float(row.get("amount"))
        if amount <= 0:
            return False, "amount_must_be_positive"
    except (TypeError, ValueError):
        return False, "invalid_amount"

    category = row.get("category")
    if not category or category not in ALLOWED_CATEGORIES:
        return False, "invalid_category"

    date_str = row.get("date")
    try:
        datetime.fromisoformat(date_str)
    except Exception:
        return False, "invalid_date"

    return True, None
