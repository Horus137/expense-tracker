#!/bin/bash

echo "Generating fake expense data..."
python -m ingestion.generate_fake_expenses

echo "Running category aggregation..."
python -m transformations.aggregate_expenses

echo "Running monthly aggregation..."
python -m transformations.aggregate_monthly

echo "Launching Jupyter notebook..."
jupyter notebook

echo "Pipeline execution completed."