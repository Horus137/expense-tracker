from datetime import datetime, timezone
from pymongo import UpdateOne
from database.mongo_client import get_db


def aggregate_by_category():
    db = get_db()

    pipeline = [
        {
            "$group": {
                "_id": "$category",
                "total_spent": {"$sum": "$amount"},
                "count": {"$sum": 1},
                "avg_spent": {"$avg": "$amount"},
            }
        },
        {"$sort": {"total_spent": -1}},
    ]

    results = list(db.expenses.aggregate(pipeline))

    run_ts = datetime.now(timezone.utc)
    for doc in results:
        doc["aggregation_level"] = "category"
        doc["run_ts"] = run_ts

    return results


def main():
    db = get_db()

    print("Running category aggregation...")

    results = aggregate_by_category()

    if not results:
        print("No data to aggregate.")
        return

    operations = []

    for doc in results:
        operations.append(
            UpdateOne(
                {"_id": doc["_id"]},   # category name
                {"$set": doc},
                upsert=True
            )
        )

    if operations:
        db.category_summary.bulk_write(operations, ordered=False)

    print(f"Saved {len(results)} records to category_summary")
    print(f"STATS: rows={len(results)}")


if __name__ == "__main__":
    main()
