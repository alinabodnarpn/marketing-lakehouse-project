"""Repeat Purchase Rate by market segment."""

from pyspark.sql import functions as F

from config.settings import CUSTOMER_GOLD, RPR_GOLD


def calculate_repeat_purchase_rate(spark):
    """
    RPR = customers with more than 1 order / all customers in the segment.
    Writes RPR_GOLD and returns the segment-level DataFrame ordered by RPR desc.
    """
    gold = spark.table(CUSTOMER_GOLD)

    rpr = (
        gold.groupBy("market_segment")
        .agg(
            F.count("*").alias("total_customers"),
            F.sum(F.when(F.col("is_repeat_customer"), 1).otherwise(0)).alias(
                "repeat_customers"
            ),
        )
        .withColumn(
            "repeat_purchase_rate",
            F.col("repeat_customers") / F.col("total_customers"),
        )
    )

    rpr.write.mode("overwrite").saveAsTable(RPR_GOLD)
    print(f"Built {RPR_GOLD}")

    return spark.table(RPR_GOLD).orderBy(F.col("repeat_purchase_rate").desc())


def highest_repeat_purchase_segment(spark):
    """Return (market_segment, repeat_purchase_rate) for the top segment."""
    row = (
        spark.table(RPR_GOLD)
        .orderBy(F.col("repeat_purchase_rate").desc())
        .limit(1)
        .collect()[0]
    )
    return row["market_segment"], float(row["repeat_purchase_rate"])
