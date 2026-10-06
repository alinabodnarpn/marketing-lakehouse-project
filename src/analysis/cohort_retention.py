"""Q4: 3-month retention of monthly first-order cohorts (1996)."""

from pyspark.sql import Window
from pyspark.sql import functions as F

from config.settings import COHORT_RETENTION_GOLD, ORDERS_GOLD_SOURCE

RETENTION_MONTHS = 3


def calculate_cohort_retention(spark, year=1996, months=RETENTION_MONTHS):
    """
    Cohort = month of the customer's first order.
    Returned = the customer's second order (by date, then order key) was placed
    no later than first_order_date + `months` months (inclusive).
    A second order on the same day as the first one counts as returned.

    Writes COHORT_RETENTION_GOLD and returns it ordered by cohort.
    """
    orders = spark.table(ORDERS_GOLD_SOURCE)

    order_rank = Window.partitionBy("o_custkey").orderBy("o_orderdate", "o_orderkey")

    # One row per customer: first and second order dates (second is NULL
    # for one-time buyers).
    customer_orders = (
        orders
        .withColumn("order_number", F.row_number().over(order_rank))
        .filter(F.col("order_number") <= 2)
        .groupBy(F.col("o_custkey").alias("customer_key"))
        .agg(
            F.min(F.when(F.col("order_number") == 1, F.col("o_orderdate"))).alias(
                "first_order_date"
            ),
            F.min(F.when(F.col("order_number") == 2, F.col("o_orderdate"))).alias(
                "second_order_date"
            ),
        )
        .withColumn("cohort_month", F.trunc("first_order_date", "month"))
        .withColumn(
            "returned_within_window",
            F.col("second_order_date").isNotNull()
            & (
                F.col("second_order_date")
                <= F.add_months(F.col("first_order_date"), months)
            ),
        )
    )

    data_end = orders.agg(F.max("o_orderdate")).collect()[0][0]

    retention = (
        customer_orders
        .filter(F.year("cohort_month") == year)
        .groupBy("cohort_month")
        .agg(
            F.count("*").alias("cohort_customers"),
            F.sum(F.when(F.col("returned_within_window"), 1).otherwise(0)).alias(
                "returned_customers"
            ),
        )
        .withColumn(
            "retention_rate",
            F.col("returned_customers") / F.col("cohort_customers"),
        )
        # Last day of the observation window for the latest customer in the cohort.
        # If data ends earlier, the cohort's rate is understated.
        .withColumn(
            "is_window_complete",
            F.add_months(F.last_day("cohort_month"), months) <= F.lit(data_end),
        )
        .withColumn("cohort", F.date_format("cohort_month", "yyyy-MM"))
        .select(
            "cohort",
            "cohort_month",
            "cohort_customers",
            "returned_customers",
            "retention_rate",
            "is_window_complete",
        )
    )

    retention.write.mode("overwrite").saveAsTable(COHORT_RETENTION_GOLD)
    print(f"Built {COHORT_RETENTION_GOLD}")

    return spark.table(COHORT_RETENTION_GOLD).orderBy("cohort_month")
