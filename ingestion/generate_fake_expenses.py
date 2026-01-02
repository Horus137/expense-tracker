import random
from datetime import datetime, timedelta
from faker import Faker

from database.mongo_client import get_db

fake = Faker()

CATEGORIES = ["food", "transport", "rent", "entertainment", "health", "shopping"]
MERCHANTS = ["Continente", "Pingo Doce", "Uber", "Netflix", "Amazon", "Farmácia"]
PAYMENT_METHODS = ["card", "cash", "mbway"]


def generate_expense():
    return {
        "user_id": "user_1",
        "amount": round(random.uniform(2, 150), 2),
        "currency": "EUR",
        "category": random.choice(CATEGORIES),
        "merchant": random.choice(MERCHANTS),
        "payment_method": random.choice(PAYMENT_METHODS),
        "timestamp": fake.date_time_between(
            start_date="-60d", end_date="now"
        ),
        "ingestion_ts": datetime.utcnow(),
    }


def main():
    db = get_db()

    expenses = [generate_expense() for _ in range(100)]

    result = db.expenses.insert_many(expenses)
    print(f"Inserted {len(result.inserted_ids)} fake expenses")


if __name__ == "__main__":
    main()
