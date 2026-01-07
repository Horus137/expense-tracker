import pandas as pd
from datetime import datetime, timezone
from database.mongo_client import get_db
from validation.validate_expense import validate_expense
from pymongo import UpdateOne


CSV_PATH = "data/input/bank_expenses.csv"
LOG_PATH = "data/logs/rejected_rows.csv"

REQUIRED_FIELDS = ["date", "amount", "category", "merchant", "payment_method"]


def validate_row(row):
    """
    CSV-level validation (structure & presence).
    Business rules are validated in validate_expense().
    """

    # Required fields present
    for field in REQUIRED_FIELDS:
        if pd.isna(row.get(field)):
            return False, "missing_required_field"

    if not str(row["merchant"]).strip():
        return False, "empty_merchant"

    if not str(row["payment_method"]).strip():
        return False, "empty_payment_method"

    # Delegate core validation
    expense_dict = {
        "amount": row["amount"],
        "category": row["category"],
        "date": row["date"],
    }

    return validate_expense(expense_dict)


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
            rejected["rejected_at"] = datetime.now(timezone.utc)
            rejected_rows.append(rejected)
            continue

        valid_expenses.append({
            "user_id": "user_1",
            "amount": float(row["amount"]),
            "currency": "EUR",
            "category": row["category"],
            "merchant": row["merchant"],
            "payment_method": row["payment_method"],
            "timestamp": pd.to_datetime(row["date"]),
            "ingestion_ts": datetime.now(timezone.utc),
            "source": "csv",
        })

    if valid_expenses:
        operations = []

        for expense in valid_expenses:
            operations.append(
                UpdateOne(
                    {
                        "user_id": expense["user_id"],
                        "amount": expense["amount"],
                        "timestamp": expense["timestamp"],
                        "merchant": expense["merchant"],
                    },
                    {"$setOnInsert": expense},
                    upsert=True,
                )
            )

        result = db.expenses.bulk_write(operations, ordered=False)

        print(
            f"Upserted {result.upserted_count} new rows "
            f"(matched {result.matched_count})"
        )

    if rejected_rows:
        pd.DataFrame(rejected_rows).to_csv(LOG_PATH, index=False)

    print(f"Inserted {len(valid_expenses)} valid rows")
    print(f"Rejected {len(rejected_rows)} rows (logged to {LOG_PATH})")
    print(f"STATS: inserted={len(valid_expenses)}, rejected={len(rejected_rows)}")


if __name__ == "__main__":
    main()
