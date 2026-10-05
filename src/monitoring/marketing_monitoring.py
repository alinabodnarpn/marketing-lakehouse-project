"""Monitoring: activation rate by segment and new customers per month.

Both tables are built from Gold `customer_marketing`, one row per month.
Only complete months are kept (the last month of data is dropped if the data
ends mid-month), so period-over-period comparisons never use partial periods.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from config.settings import (
    ACTIVATION_MONITORING_GOLD,
    CUSTOMER_GOLD,
    NEW_CUSTOMERS_MONITORING_GOLD,
    ORDERS_GOLD_SOURCE,
)

# Alert: activation rate of a segment fell by more than this many percentage
# points vs the previous month. The rate is cumulative, so with a stable
# customer base any drop means customers were added without orders or orders
# were lost upstream.
ACTIVATION_DROP_PP = 1.0

# Alert: new customers in a month fell below this share of the trailing
# 3-month average. Months whose trailing average is below the minimum are
# skipped: at a handful of customers per month a 50% drop is just noise.
NEW_CUSTOMERS_DROP_RATIO = 0.5
NEW_CUSTOMERS_MIN_TRAILING_AVG = 10
TRAILING_MONTHS = 3


def _complete_months(spark):
    """One row per calendar month from the first order month to the last complete one."""
    bounds = spark.table(ORDERS_GOLD_SOURCE).agg(
        F.trunc(F.min("o_orderdate"), "month").alias("start_month"),
        F.max("o_orderdate").alias("data_end"),
    )
    return (
        bounds
        .select(
            F.explode(
                F.sequence("start_month", F.trunc("data_end", "month"), F.expr("interval 1 month"))
            ).alias("month"),
            "data_end",
        )
        .filter(F.last_day("month") <= F.col("data_end"))
        .select("month")
    )


def build_activation_monitoring(spark):
    """
    activation_rate = customers in the segment with first_order_date <= month end
                      / all customers in the segment.
    """
    customers = spark.table(CUSTOMER_GOLD)
    months = _complete_months(spark)

    segment_totals = customers.groupBy("market_segment").agg(
        F.count("*").alias("total_customers")
    )

    activated_by_month = (
        customers
        .filter(F.col("first_order_date").isNotNull())
        .groupBy("market_segment", F.trunc("first_order_date", "month").alias("month"))
        .agg(F.count("*").alias("newly_activated"))
    )

    by_segment = Window.partitionBy("market_segment").orderBy("month")

    monitoring = (
        months.crossJoin(segment_totals)
        .join(activated_by_month, ["market_segment", "month"], "left")
        .fillna(0, subset=["newly_activated"])
        .withColumn(
            "activated_customers",
            F.sum("newly_activated").over(
                by_segment.rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
        .withColumn(
            "activation_rate",
            F.col("activated_customers") / F.col("total_customers"),
        )
        .withColumn(
            "change_pp",
            (F.col("activation_rate") - F.lag("activation_rate").over(by_segment)) * 100,
        )
        .withColumn(
            "is_alert",
            F.coalesce(F.col("change_pp") < -ACTIVATION_DROP_PP, F.lit(False)),
        )
        .select(
            "month",
            "market_segment",
            "total_customers",
            "newly_activated",
            "activated_customers",
            "activation_rate",
            "change_pp",
            "is_alert",
        )
    )

    monitoring.write.mode("overwrite").saveAsTable(ACTIVATION_MONITORING_GOLD)
    print(f"Built {ACTIVATION_MONITORING_GOLD}")
    return spark.table(ACTIVATION_MONITORING_GOLD).orderBy("market_segment", "month")


def build_new_customers_monitoring(spark):
    """New customers (by first order date) per month, with a drop alert."""
    customers = spark.table(CUSTOMER_GOLD)
    months = _complete_months(spark)

    new_by_month = (
        customers
        .filter(F.col("first_order_date").isNotNull())
        .groupBy(F.trunc("first_order_date", "month").alias("month"))
        .agg(F.count("*").alias("new_customers"))
    )

    by_month = Window.orderBy("month")

    monitoring = (
        months
        .join(new_by_month, "month", "left")
        .fillna(0, subset=["new_customers"])
        .withColumn(
            "quarter",
            F.concat_ws("-Q", F.year("month").cast("string"), F.quarter("month").cast("string")),
        )
        .withColumn(
            "trailing_avg",
            F.avg("new_customers").over(by_month.rowsBetween(-TRAILING_MONTHS, -1)),
        )
        .withColumn(
            "is_alert",
            F.coalesce(
                (F.col("trailing_avg") >= NEW_CUSTOMERS_MIN_TRAILING_AVG)
                & (F.col("new_customers") < F.col("trailing_avg") * NEW_CUSTOMERS_DROP_RATIO),
                F.lit(False),
            ),
        )
        .select("month", "quarter", "new_customers", "trailing_avg", "is_alert")
    )

    monitoring.write.mode("overwrite").saveAsTable(NEW_CUSTOMERS_MONITORING_GOLD)
    print(f"Built {NEW_CUSTOMERS_MONITORING_GOLD}")
    return spark.table(NEW_CUSTOMERS_MONITORING_GOLD).orderBy("month")


def build_monitoring(spark):
    return build_activation_monitoring(spark), build_new_customers_monitoring(spark)


def active_alerts(spark):
    """All alert rows from both monitoring tables, newest first."""
    activation = (
        spark.table(ACTIVATION_MONITORING_GOLD)
        .filter("is_alert")
        .select(
            "month",
            F.lit("activation_rate_drop").alias("alert"),
            F.col("market_segment").alias("scope"),
            F.round("change_pp", 2).cast("string").alias("detail"),
        )
    )
    new_customers = (
        spark.table(NEW_CUSTOMERS_MONITORING_GOLD)
        .filter("is_alert")
        .select(
            "month",
            F.lit("new_customers_drop").alias("alert"),
            F.lit("ALL").alias("scope"),
            F.concat_ws(
                " vs avg ",
                F.col("new_customers").cast("string"),
                F.round("trailing_avg", 1).cast("string"),
            ).alias("detail"),
        )
    )
    return activation.unionByName(new_customers).orderBy(F.col("month").desc())
