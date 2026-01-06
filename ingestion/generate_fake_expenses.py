import random
from datetime import datetime, timezone
from faker import Faker
from pymongo import UpdateOne

from database.mongo_client import get_db

fake = Faker()

CATEGORIES = ["food", "transport", "rent", "entertainment", "health", "shopping"]
MERCHANTS = ["Continente", "Pingo Doce", "Uber", "Netflix", "Amazon", "Farmácia"]
PAYMENT_METHODS = ["card", "cash", "mbway"]


def generate_expense():
    timestamp = fake.date_time_between(start_date="-60d", end_date="now")

    return {
        "user_id": "user_1",
        "amount": round(random.uniform(2, 150), 2),
        "currency": "EUR",
        "category": random.choice(CATEGORIES),
        "merchant": random.choice(MERCHANTS),
        "payment_method": random.choice(PAYMENT_METHODS),
        "timestamp": timestamp,
        "ingestion_ts": datetime.now(timezone.utc),
        "source": "faker",
    }


def main():
    db = get_db()

    expenses = [generate_expense() for _ in range(100)]

    operations = []

    for expense in expenses:
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

    if operations:
        result = db.expenses.bulk_write(operations, ordered=False)

        print(
            f"Upserted {result.upserted_count} new expenses "
            f"(matched {result.matched_count})"
        )


if __name__ == "__main__":
    main()
