#!/bin/bash
set -e

echo "Running ingestion"
python -m ingestion.generate_fake_expenses
python -m ingestion.ingest_csv_expenses

echo "Running transformations"
python -m transformations.aggregate_expenses
python -m transformations.aggregate_monthly

echo "Pipeline finished"
