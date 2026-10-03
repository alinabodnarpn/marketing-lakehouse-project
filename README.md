# marketing-lakehouse-project

TPC-H Marketing Lakehouse on Databricks (UCU).

Pipeline: **Bronze → Silver → Gold → Business Analytics**

## Schemas (`config/settings.py`)

| Layer | Schema | Tables |
|-------|--------|--------|
| Source | `samples.tpch` | `customer`, `orders` |
| Bronze | `workspace.marketing_bronze` | `customer`, `orders` |
| Silver | `workspace.marketing_silver` | `customer`, `orders` |
| Gold | `workspace.marketing_gold` | `customer_marketing`, `repeat_purchase_rate_by_segment` |

## Layout

```
config/settings.py          # all table names — change sources here
src/bronze/                 # ingest TPC-H → Bronze
src/silver/                 # quality checks → Silver
src/gold/                   # customer marketing Gold table
src/analysis/               # activation, new customers, RPR
src/validation/             # cross-table checks
tests/                      # pytest (run via notebooks/run_tests.ipynb)
notebooks/                  # thin Databricks notebooks
```

## How to run (order)

1. Bronze: `src/bronze/load_customer.py`, `src/bronze/load_orders.py`
2. Silver: `src/silver/customer.py`, `src/silver/orders.py`
3. Gold: `notebooks/repeat_purchase_rate_analysis.ipynb`
4. Tests: `notebooks/run_tests.ipynb`

## Analytics

| Question | Module / notebook |
|----------|-------------------|
| Activation rate | `src/analysis/activation_rate.py` |
| New customers (1996-Q1) | `src/analysis/new_customers.py` |
| Highest Repeat Purchase Rate by segment | `src/analysis/repeat_purchase_rate.py` |

Gold reads Silver via `CUSTOMER_GOLD_SOURCE` / `ORDERS_GOLD_SOURCE` in settings.
