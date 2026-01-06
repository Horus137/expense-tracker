from datetime import datetime, timezone
from database.mongo_client import get_db


def aggregate_by_month():
    db = get_db()

    pipeline = [
        {
            "$group": {
                "_id": {
                    "year": {"$year": "$timestamp"},
                    "month": {"$month": "$timestamp"},
                },
                "total_spent": {"$sum": "$amount"},
                "count": {"$sum": 1},
                "avg_spent": {"$avg": "$amount"},
            }
        },
        {
            "$sort": {
                "_id.year": 1,
                "_id.month": 1,
            }
        },
    ]

    results = list(db.expenses.aggregate(pipeline))

    run_ts = datetime.now(timezone.utc)
    for doc in results:
        doc["aggregation_level"] = "month"
        doc["run_ts"] = run_ts

    return results


def main():
    db = get_db()

    print("Running monthly aggregation...")

    results = aggregate_by_month()

    if not results:
        print("No data to aggregate.")
        return

    db.monthly_summary.delete_many({})
    db.monthly_summary.insert_many(results)

    print(f"Saved {len(results)} records to monthly_summary")


if __name__ == "__main__":
    main()
