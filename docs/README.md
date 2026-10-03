# Marketing Lakehouse Project

This project implements a Lakehouse architecture in Databricks using the Bronze, Silver, and Gold layers.

The project uses the TPC-H dataset and focuses on customer and order data to answer marketing-related business questions.

## Architecture

The project follows the Medallion Architecture:

- **Bronze** — raw data loaded from the source without transformations.
- **Silver** — validated and normalized data prepared for further analysis.
- **Gold** — business-oriented data used to answer analytical questions.

Data flow:

`samples.tpch → Bronze → Silver → Gold → Business Analysis`

## Silver Layer

The Silver layer contains validated and normalized customer and order data.

The following tables are created:

- `workspace.marketing_silver.customer`
- `workspace.marketing_silver.orders`

The tables preserve separate customer and order entities and are organized in Third Normal Form (3NF).

### Customer

Primary key:

`c_custkey`

The table contains customer information including name, address, account balance, market segment, and other customer attributes.

### Orders

Primary key:

`o_orderkey`

Foreign key:

`o_custkey → customer.c_custkey`

The table contains information about customer orders, including order date, total price, status, priority, and other order attributes.

### Silver ER Diagram

![Silver Layer ER Diagram](silver_er_diagram.png)

## Data Quality Validation

Data quality checks are performed before and after creating the Silver tables.

The following checks are implemented:

- Customer keys must not be NULL.
- Customer keys must be unique.
- Order keys must not be NULL.
- Order keys must be unique.
- Every order must reference an existing customer.

The customer reference validation checks whether every `o_custkey` from the orders table exists as a `c_custkey` in the customer table.

The validation uses a `left_anti` join to identify orders without a matching customer.

Validation result:

```text
Validation PASSED
Every order has a valid customer reference.
Invalid customer references: 0
```

## Business Question 2 — New Customers in 1996 Q1

**Question:**

How many new customers, based on their first order date, appeared in 1996 Q1?

A customer is considered new in the quarter if their first-ever order was placed between January 1, 1996 and March 31, 1996.

For each customer, the first order date is calculated as:

```text
MIN(o_orderdate)
```

Customers are then filtered using the following period:

```text
1996-01-01 <= first_order_date < 1996-04-01
```

### Result

**157 new customers** appeared in 1996 Q1.

Monthly breakdown:

| Month | New Customers |
|---|---:|
| January 1996 | 54 |
| February 1996 | 44 |
| March 1996 | 59 |
| **Total** | **157** |

The monthly distribution can also be visualized as a bar chart in the Databricks analysis notebook.

## Relevant Files

Silver transformation:

```text
src/silver/customer.py
src/silver/orders.py
```

Business question analysis:

```text
src/analysis/new_customers.py
```

Validation:

```text
src/validation/order_customer_reference.py
```

Databricks notebooks:

```text
notebooks/new_customers.ipynb
notebooks/validate_order_customer_reference.ipynb
```

ER diagram:

```text
docs/silver_er_diagram.png
```