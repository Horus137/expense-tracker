#!/bin/bash
set -e

# --------------------------------------
# Start pipeline run
# --------------------------------------
RUN_ID=$(python - <<EOF
from pipeline.run_context import start_run
print(start_run())
EOF
)

echo "▶ Pipeline run id: $RUN_ID"

fail_pipeline () {
  STEP=$1
  ERROR=$2

  python - <<EOF
from pipeline.run_context import finish_step, finish_run
finish_step("$RUN_ID", "$STEP", status="failed")
finish_run("$RUN_ID", status="failed", error=f"$STEP: $ERROR")
EOF

  echo "❌ Pipeline failed at step: $STEP"
  exit 1
}

# --------------------------------------
# Fake ingestion
# --------------------------------------
echo "▶ Running fake ingestion"

python - <<EOF
from pipeline.run_context import start_step
start_step("$RUN_ID", "fake_ingestion")
EOF

OUT=$(python -m ingestion.generate_fake_expenses) || fail_pipeline "fake_ingestion" "execution failed"
echo "$OUT"

FAKE_COUNT=$(echo "$OUT" | grep STATS | sed 's/.*inserted=//')

python - <<EOF
from pipeline.run_context import finish_step
finish_step("$RUN_ID", "fake_ingestion", {
    "inserted": int($FAKE_COUNT)
})
EOF

# --------------------------------------
# CSV ingestion
# --------------------------------------
echo "▶ Running CSV ingestion"

python - <<EOF
from pipeline.run_context import start_step
start_step("$RUN_ID", "csv_ingestion")
EOF

OUT=$(python -m ingestion.ingest_csv_expenses) || fail_pipeline "csv_ingestion" "execution failed"
echo "$OUT"

INSERTED=$(echo "$OUT" | grep STATS | sed 's/.*inserted=\([0-9]*\).*/\1/')
REJECTED=$(echo "$OUT" | grep STATS | sed 's/.*rejected=\([0-9]*\).*/\1/')

python - <<EOF
from pipeline.run_context import finish_step
finish_step("$RUN_ID", "csv_ingestion", {
    "inserted": int($INSERTED),
    "rejected": int($REJECTED)
})
EOF

# --------------------------------------
# Category aggregation
# --------------------------------------
echo "▶ Running category aggregation"

python - <<EOF
from pipeline.run_context import start_step
start_step("$RUN_ID", "category_aggregation")
EOF

OUT=$(python -m transformations.aggregate_expenses) || fail_pipeline "category_aggregation" "execution failed"
echo "$OUT"

ROWS=$(echo "$OUT" | grep STATS | sed 's/.*rows=//')

python - <<EOF
from pipeline.run_context import finish_step
finish_step("$RUN_ID", "category_aggregation", {
    "rows": int($ROWS)
})
EOF

# --------------------------------------
# Monthly aggregation
# --------------------------------------
echo "▶ Running monthly aggregation"

python - <<EOF
from pipeline.run_context import start_step
start_step("$RUN_ID", "monthly_aggregation")
EOF

OUT=$(python -m transformations.aggregate_monthly) || fail_pipeline "monthly_aggregation" "execution failed"
echo "$OUT"

ROWS=$(echo "$OUT" | grep STATS | sed 's/.*rows=//')

python - <<EOF
from pipeline.run_context import finish_step
finish_step("$RUN_ID", "monthly_aggregation", {
    "rows": int($ROWS)
})
EOF

# --------------------------------------
# Finish pipeline
# --------------------------------------
python - <<EOF
from pipeline.run_context import finish_run
finish_run("$RUN_ID", status="success")
EOF

echo "✅ Pipeline finished successfully"
