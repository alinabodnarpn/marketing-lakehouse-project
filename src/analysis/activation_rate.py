from pyspark.sql import functions as F

from config.settings import CUSTOMER_GOLD


def calculate_activation_rate(spark, segment="BUILDING"):
    customers = spark.table(CUSTOMER_GOLD)

    result = (
        customers
        .filter(F.col("market_segment") == segment)
        .agg(
            F.sum(
                F.when(F.col("is_activated"), 1).otherwise(0)
            ).alias("activated_customers"),

            F.count("*").alias("total_customers"),
        )
        .withColumn(
            "activation_rate_percent",
            F.round(
                F.col("activated_customers")
                / F.col("total_customers")
                * 100,
                2,
            ),
        )
    )

    return result