from config.settings import CUSTOMER_BRONZE, CUSTOMER_SILVER
from pyspark.sql import functions as F

customer_df = spark.table(CUSTOMER_BRONZE)

customer_df.printSchema()

null_customer_keys = customer_df.filter(
    F.col("c_custkey").isNull()
).count()

duplicate_customer_keys = (
    customer_df
    .groupBy("c_custkey")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"NULL customer keys: {null_customer_keys}")
print(f"Duplicate customer keys: {duplicate_customer_keys}")

if null_customer_keys > 0:
    raise ValueError("Customer data quality failed: c_custkey contains NULL values")
if duplicate_customer_keys > 0:
    raise ValueError("Customer data quality failed: c_custkey contains duplicates")

customer_df.write \
    .mode("overwrite") \
    .saveAsTable(CUSTOMER_SILVER)
print(f"Loaded {CUSTOMER_BRONZE} -> {CUSTOMER_SILVER}")