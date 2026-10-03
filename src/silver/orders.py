from config.settings import (
    ORDERS_BRONZE,
    ORDERS_SILVER,
    CUSTOMER_SILVER,
)
from pyspark.sql import functions as F

orders_df = spark.table(ORDERS_BRONZE)

orders_df.printSchema()

null_order_keys = orders_df.filter(
    F.col("o_orderkey").isNull()
).count()

duplicate_order_keys = (
    orders_df
    .groupBy("o_orderkey")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"NULL order keys: {null_order_keys}")
print(f"Duplicate order keys: {duplicate_order_keys}")

customers_df = spark.table(CUSTOMER_SILVER)
invalid_customer_references = (
    orders_df
    .join(
        customers_df,
        orders_df["o_custkey"] == customers_df["c_custkey"],
        "left_anti"
    )
    .count()
)
print(f"Orders with invalid customer reference: {invalid_customer_references}")

if null_order_keys > 0:
    raise ValueError("Orders data quality failed: o_orderkey contains NULL values")
if duplicate_order_keys > 0:
    raise ValueError("Orders data quality failed: o_orderkey contains duplicates")
if invalid_customer_references > 0:
    raise ValueError(
        "Orders data quality failed: some orders reference non-existing customers"
    )
orders_df.write \
    .mode("overwrite") \
    .saveAsTable(ORDERS_SILVER)
print(f"Loaded {ORDERS_BRONZE} -> {ORDERS_SILVER}")