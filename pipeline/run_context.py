import uuid
from datetime import datetime, timezone
from database.mongo_client import get_db


def utc_now():
    return datetime.now(timezone.utc)


def start_run():
    db = get_db()
    run_id = str(uuid.uuid4())

    db.pipeline_runs.insert_one({
        "_id": run_id,
        "started_at": utc_now(),
        "status": "running",
        "steps": {}
    })

    return run_id


def start_step(run_id, step_name):
    db = get_db()
    db.pipeline_runs.update_one(
        {"_id": run_id},
        {"$set": {
            f"steps.{step_name}": {
                "status": "running",
                "started_at": utc_now()
            }
        }}
    )


def finish_step(run_id, step_name, metrics=None, status="success"):
    db = get_db()
    finished_at = utc_now()

    run = db.pipeline_runs.find_one({"_id": run_id})
    step = run["steps"].get(step_name)

    duration = None
    if step and "started_at" in step:
        started_at = step["started_at"]

        # 🔐 backward-safe timezone fix
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=timezone.utc)

        duration = (finished_at - started_at).total_seconds()

    update = {
        f"steps.{step_name}.status": status,
        f"steps.{step_name}.finished_at": finished_at,
        f"steps.{step_name}.duration_sec": duration
    }

    if metrics:
        update[f"steps.{step_name}.metrics"] = metrics

    db.pipeline_runs.update_one(
        {"_id": run_id},
        {"$set": update}
    )



def finish_run(run_id, status="success", error=None):
    db = get_db()
    finished_at = utc_now()

    run = db.pipeline_runs.find_one({"_id": run_id})
    steps = run.get("steps", {})

    total_inserted = 0
    total_rejected = 0

    for step in steps.values():
        metrics = step.get("metrics", {})
        total_inserted += metrics.get("inserted", 0)
        total_rejected += metrics.get("rejected", 0)

    update = {
        "finished_at": finished_at,
        "status": status,
        "metrics": {
            "total_inserted": total_inserted,
            "total_rejected": total_rejected
        }
    }

    if error:
        update["error"] = str(error)

    db.pipeline_runs.update_one(
        {"_id": run_id},
        {"$set": update}
    )
