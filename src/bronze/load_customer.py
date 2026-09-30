from config.settings import CUSTOMER_SOURCE, CUSTOMER_BRONZE

customer_df = spark.table(CUSTOMER_SOURCE)
customer_df.write.mode("overwrite").saveAsTable(CUSTOMER_BRONZE)

print(f"Loaded {CUSTOMER_SOURCE} -> {CUSTOMER_BRONZE}")