from datetime import datetime, timezone
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

    # Add metadata
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

    # Replace analytics table
    db.category_summary.delete_many({})
    db.category_summary.insert_many(results)

    print(f"Saved {len(results)} records to category_summary")


if __name__ == "__main__":
    main()
