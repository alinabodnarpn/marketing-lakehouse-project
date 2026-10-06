# Notebooks

Thin wrappers over `src/`. Run from a Databricks Git folder of this repo.

| Notebook | Layer | Purpose |
|----------|-------|---------|
| `check_tpch.ipynb` | source | Peek at TPC-H samples |
| `check_silver.ipynb` | silver | Inspect Silver tables |
| `validate_order_customer_reference.ipynb` | silver | FK check orders → customers |
| `activation_rate_analysis.ipynb.ipynb` | analysis | Activation rate |
| `new_customers.ipynb` | analysis | New customers 1996-Q1 |
| `repeat_purchase_rate_analysis.ipynb` | gold | Gold build + RPR by segment |
| `cohort_retention_monitoring.ipynb` | gold | 1996 cohort retention + monitoring + alerts |
| `run_tests.ipynb` | all | pytest for Bronze + Gold + cohorts/monitoring |
| `run_pipeline.ipynb` | all | End-to-end run Bronze → Gold → analytics → tests |
