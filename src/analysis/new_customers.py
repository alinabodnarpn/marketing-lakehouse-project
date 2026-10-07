from pyspark.sql import functions as F

from config.settings import CUSTOMER_GOLD


def calculate_new_customers(spark):
    customers = spark.table(CUSTOMER_GOLD)

    # Customers whose first-ever order was in 1996-Q1
    new_customers_q1 = (
        customers
        .filter(
            (F.col("first_order_date") >= F.lit("1996-01-01"))
            & (F.col("first_order_date") < F.lit("1996-04-01"))
        )
    )

    result = (
        new_customers_q1
        .agg(
            F.count("*").alias("new_customers")
        )
    )

    return result