import pandas as pd
from datetime import datetime
from database.mongo_client import get_db


CSV_PATH = "data/input/bank_expenses.csv"
LOG_PATH = "data/logs/rejected_rows.csv"

REQUIRED_FIELDS = ["date", "amount", "category", "merchant", "payment_method"]


def validate_row(row):
    if any(pd.isna(row[field]) for field in REQUIRED_FIELDS):
        return False, "missing_required_field"

    try:
        amount = float(row["amount"])
        if amount <= 0:
            return False, "invalid_amount"
    except Exception:
        return False, "amount_not_numeric"

    try:
        pd.to_datetime(row["date"])
    except Exception:
        return False, "invalid_date"

    if not str(row["category"]).strip():
        return False, "empty_category"

    if not str(row["merchant"]).strip():
        return False, "empty_merchant"

    return True, None


def main():
    db = get_db()
    df = pd.read_csv(CSV_PATH)

    valid_expenses = []
    rejected_rows = []

    for _, row in df.iterrows():
        is_valid, reason = validate_row(row)

        if not is_valid:
            rejected = row.to_dict()
            rejected["rejection_reason"] = reason
            rejected["rejected_at"] = datetime.utcnow()
            rejected_rows.append(rejected)
            continue

        expense = {
            "user_id": "user_1",
            "amount": float(row["amount"]),
            "currency": "EUR",
            "category": row["category"],
            "merchant": row["merchant"],
            "payment_method": row["payment_method"],
            "timestamp": pd.to_datetime(row["date"]),
            "ingestion_ts": datetime.utcnow(),
            "source": "csv",
        }

        valid_expenses.append(expense)

    if valid_expenses:
        db.expenses.insert_many(valid_expenses)

    if rejected_rows:
        pd.DataFrame(rejected_rows).to_csv(LOG_PATH, index=False)

    print(f"Inserted {len(valid_expenses)} valid rows")
    print(f"Rejected {len(rejected_rows)} rows (logged to {LOG_PATH})")


if __name__ == "__main__":
    main()
