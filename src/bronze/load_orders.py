from config.settings import ORDERS_SOURCE, ORDERS_BRONZE

orders_df = spark.table(ORDERS_SOURCE)
orders_df.write.mode("overwrite").saveAsTable(ORDERS_BRONZE)

print(f"Loaded {ORDERS_SOURCE} -> {ORDERS_BRONZE}")