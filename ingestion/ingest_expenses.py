from datetime import datetime, timezone
from database.mongo_client import get_db

def main():
    db = get_db()

    expense = {
        "user_id": "user_1",
        "amount": 12.50,
        "currency": "EUR",
        "category": "food",
        "merchant": "Continente",
        "payment_method": "card",
        "timestamp": datetime.now(timezone.utc),
        "ingestion_ts": datetime.now(timezone.utc)
    }

    result = db.expenses.insert_one(expense)
    print("Inserted expense with id:", result.inserted_id)

if __name__ == "__main__":
    main()
