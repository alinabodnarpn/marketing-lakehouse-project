from pyspark.sql import functions as F

from config.settings import (
    CUSTOMER_SILVER,
    ORDERS_SILVER,
)


def calculate_activation_rate(spark, segment="BUILDING"):
    customers = spark.table(CUSTOMER_SILVER)
    orders = spark.table(ORDERS_SILVER)

    filtered_customers = (
        customers
        .filter(F.col("c_mktsegment") == segment)
        .join(
            orders,
            customers.c_custkey == orders.o_custkey,
            "left",
        )
        .groupBy(customers.c_custkey)
        .agg(
            F.count(orders.o_orderkey).alias("orders")
        )
    )

    result = (
        filtered_customers
        .agg(
            F.sum(
                F.when(F.col("orders") > 0, 1).otherwise(0)
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