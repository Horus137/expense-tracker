# Expense Tracker – Data Engineering Project

A small end-to-end **data engineering pipeline** built with Python, MongoDB, and Docker.

The project simulates how raw financial data can be ingested, validated, transformed, and analyzed using an **ELT-style workflow**, with observability and metrics.

---

## What this project demonstrates

- Raw data ingestion from multiple sources (fake generator + CSV)
- Centralized storage of raw events in MongoDB
- Validation and rejection of bad records
- Transformation into analytics-ready tables
- Pipeline execution tracking (runs, steps, failures)
- Basic analytics and visualization with Jupyter

This project focuses on **data engineering concepts**, not on building a user-facing app.

---

## Architecture (high level)

1. **Ingestion**
   - Generates fake expense events
   - Ingests CSV bank transactions
   - Stores raw data in `expenses`
   - Invalid rows are logged, not dropped silently

2. **Transformations**
   - Aggregates spending by category
   - Aggregates spending by month
   - Results stored in separate analytics collections

3. **Pipeline orchestration**
   - Single entrypoint (`run_pipeline.sh`)
   - Each run is tracked in MongoDB
   - Step-level metrics and failures are recorded

4. **Analytics**
   - Jupyter notebooks query MongoDB
   - Pipeline metrics and spending data are visualized

---

## Data model

### Raw
- `expenses` – raw expense events

### Analytics
- `category_summary` – spending per category
- `monthly_summary` – spending per month

### Metadata
- `pipeline_runs` – pipeline executions, steps, metrics, and failures

---

## Tech stack

- Python
- MongoDB
- Pandas
- Matplotlib
- Jupyter Notebook
- Docker & Docker Compose

---

## How to run

```bash
docker-compose up pipeline
```

This will:
- Start MongoDB
- Run the full ingestion pipeline (fake data + CSV)
- Validate and store raw expense events
- Build aggregated analytics tables
- Record pipeline runs, steps, and metrics in MongoDB

---

## Purpose

This project was built for **learning and demonstration purposes**, with a focus on:

- Data engineering pipelines
- ELT-style workflows
- Data validation and rejection handling
- Pipeline observability and metrics
- Analytics on operational data
