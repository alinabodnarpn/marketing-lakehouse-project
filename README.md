# marketing-lakehouse-project

TPC-H Marketing Lakehouse on Databricks (UCU).

Pipeline: **Bronze → Silver → Gold → Business Analytics**

## Schemas (`config/settings.py`)

| Layer | Schema | Tables |
|-------|--------|--------|
| Source | `samples.tpch` | `customer`, `orders` |
| Bronze | `workspace.marketing_bronze` | `customer`, `orders` |
| Silver | `workspace.marketing_silver` | `customer`, `orders` |
| Gold | `workspace.marketing_gold` | `customer_marketing`, `repeat_purchase_rate_by_segment`, `cohort_retention_1996`, `monitoring_activation_rate_by_segment`, `monitoring_new_customers_monthly` |

## Layout

```
config/settings.py          # all table names — change sources here
src/bronze/                 # ingest TPC-H → Bronze
src/silver/                 # quality checks → Silver
src/gold/                   # customer marketing Gold table
src/analysis/               # activation, new customers, RPR, cohort retention
src/monitoring/             # monitoring tables + alert rules
src/validation/             # cross-table checks
tests/                      # pytest (run via notebooks/run_tests.ipynb)
notebooks/                  # thin Databricks notebooks
dashboards/                 # SQL datasets for the Databricks dashboard (Gold only)
```

## How to run

**Everything at once:** `notebooks/run_pipeline.ipynb` — runs Bronze → Silver → Gold →
analytics → monitoring → all tests, and fails if any test fails (integration check).

Step by step:

1. Bronze: `src/bronze/load_customer.py`, `src/bronze/load_orders.py`
2. Silver: `src/silver/customer.py`, `src/silver/orders.py`
3. Gold: `notebooks/repeat_purchase_rate_analysis.ipynb`
4. Cohorts + monitoring: `notebooks/cohort_retention_monitoring.ipynb`
5. Tests: `notebooks/run_tests.ipynb`
6. Dashboard: create a Databricks dashboard with the datasets in `dashboards/marketing_dashboard.sql`

## Analytics

| Question | Module / notebook |
|----------|-------------------|
| Activation rate | `src/analysis/activation_rate.py` |
| New customers (1996-Q1) | `src/analysis/new_customers.py` |
| Highest Repeat Purchase Rate by segment | `src/analysis/repeat_purchase_rate.py` |
| 3-month retention of 1996 cohorts | `src/analysis/cohort_retention.py` |
| Monitoring (activation by segment, new customers per month) | `src/monitoring/marketing_monitoring.py` |

Gold reads Silver via `CUSTOMER_GOLD_SOURCE` / `ORDERS_GOLD_SOURCE` in settings.

## Monitoring & alerts

Built from Gold `customer_marketing`, one row per **complete** month (a partial last
month is dropped, so periods are always comparable).

| Table | Metric | Alert rule |
|-------|--------|-----------|
| `monitoring_activation_rate_by_segment` | customers in segment with first order ≤ month end / all customers in segment | drop > 1 pp vs previous month |
| `monitoring_new_customers_monthly` | customers whose first order falls in the month | < 50% of trailing 3-month average (only when that average ≥ 10) |

Activation rate here is cumulative, so on a stable customer base it can only grow —
any drop means customers appeared without orders or orders were lost upstream.
Thresholds are constants at the top of `src/monitoring/marketing_monitoring.py`.
