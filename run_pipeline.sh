#!/bin/bash
set -e

echo "Generating fake expense data..."
python -m ingestion.generate_fake_expenses

echo "Ingesting CSV expenses..."
python -m ingestion.ingest_csv_expenses

echo "Running category aggregation..."
python -m transformations.aggregate_expenses

echo "Running monthly aggregation..."
python -m transformations.aggregate_monthly

echo "Launching Jupyter notebook..."
jupyter notebook --ip=0.0.0.0 --no-browser --allow-root

echo "Pipeline execution completed."