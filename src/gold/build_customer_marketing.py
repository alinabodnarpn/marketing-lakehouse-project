"""Build Gold customer marketing table (one row per customer)."""

from pyspark.sql import functions as F

from config.settings import (
    CUSTOMER_GOLD,
    CUSTOMER_GOLD_SOURCE,
    ORDERS_GOLD_SOURCE,
)


def build_customer_marketing(spark):
    customers = spark.table(CUSTOMER_GOLD_SOURCE)
    orders = spark.table(ORDERS_GOLD_SOURCE)

    gold = (
        customers.alias("c")
        .join(
            orders.alias("o"),
            F.col("c.c_custkey") == F.col("o.o_custkey"),
            "left",
        )
        .groupBy(
            F.col("c.c_custkey").alias("customer_key"),
            F.col("c.c_mktsegment").alias("market_segment"),
        )
        .agg(
            F.min("o.o_orderdate").alias("first_order_date"),
            F.count("o.o_orderkey").alias("order_count"),
        )
        .withColumn("is_activated", F.col("order_count") > 0)
        .withColumn("is_repeat_customer", F.col("order_count") > 1)
    )

    gold.write.mode("overwrite").saveAsTable(CUSTOMER_GOLD)
    print(f"Built {CUSTOMER_GOLD} from {CUSTOMER_GOLD_SOURCE} + {ORDERS_GOLD_SOURCE}")
    return spark.table(CUSTOMER_GOLD)
