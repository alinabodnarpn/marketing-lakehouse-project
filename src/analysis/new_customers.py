from pyspark.sql import functions as F
from config.settings import ORDERS_SILVER

def calculate_new_customers(spark):
    orders = spark.table(ORDERS_SILVER)
    # Find the first order date for each customer
    first_orders = (
        orders
        .groupBy("o_custkey")
        .agg(
            F.min("o_orderdate").alias("first_order_date")
        )
    )
    # Keep customers whose first order was in 1996-Q1
    new_customers_q1 = (
        first_orders
        .filter(
            (F.col("first_order_date") >= F.lit("1996-01-01"))
            & (F.col("first_order_date") < F.lit("1996-04-01"))
        )
    )
    # Count new customers
    result = (
        new_customers_q1
        .agg(
            F.count("*").alias("new_customers")
        )
    )

    return result