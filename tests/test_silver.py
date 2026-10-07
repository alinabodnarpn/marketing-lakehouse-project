from pyspark.sql import SparkSession

from config.settings import (
    CUSTOMER_BRONZE,
    ORDERS_BRONZE,
    CUSTOMER_SILVER,
    ORDERS_SILVER,
)

spark = SparkSession.getActiveSession()


def test_customer_row_count_preserved():
    bronze = spark.table(CUSTOMER_BRONZE)
    silver = spark.table(CUSTOMER_SILVER)

    assert bronze.count() == silver.count()


def test_order_row_count_preserved():
    bronze = spark.table(ORDERS_BRONZE)
    silver = spark.table(ORDERS_SILVER)

    assert bronze.count() == silver.count()


def test_customer_key_uniqueness():
    customers = spark.table(CUSTOMER_SILVER)

    total_count = customers.count()
    unique_count = customers.select("c_custkey").distinct().count()

    assert total_count == unique_count


def test_order_key_uniqueness():
    orders = spark.table(ORDERS_SILVER)

    total_count = orders.count()
    unique_count = orders.select("o_orderkey").distinct().count()

    assert total_count == unique_count


def test_customer_key_not_null():
    customers = spark.table(CUSTOMER_SILVER)

    null_count = customers.filter(
        customers.c_custkey.isNull()
    ).count()

    assert null_count == 0


def test_order_key_not_null():
    orders = spark.table(ORDERS_SILVER)

    null_count = orders.filter(
        orders.o_orderkey.isNull()
    ).count()

    assert null_count == 0


def test_orders_have_valid_customer():
    orders = spark.table(ORDERS_SILVER)
    customers = spark.table(CUSTOMER_SILVER)

    invalid_orders = (
        orders
        .join(
            customers,
            orders.o_custkey == customers.c_custkey,
            "left_anti",
        )
    )

    assert invalid_orders.count() == 0


def test_all_bronze_customers_survive_in_silver():
    bronze = spark.table(CUSTOMER_BRONZE)
    silver = spark.table(CUSTOMER_SILVER)

    missing_customers = (
        bronze
        .select("c_custkey")
        .join(
            silver.select("c_custkey"),
            on="c_custkey",
            how="left_anti",
        )
    )

    assert missing_customers.count() == 0


def test_customers_without_orders_survive_in_silver():
    bronze_customers = spark.table(CUSTOMER_BRONZE)
    bronze_orders = spark.table(ORDERS_BRONZE)
    silver_customers = spark.table(CUSTOMER_SILVER)

    bronze_customers_without_orders = (
        bronze_customers
        .select("c_custkey")
        .join(
            bronze_orders.select("o_custkey"),
            bronze_customers.c_custkey == bronze_orders.o_custkey,
            "left_anti",
        )
        .select("c_custkey")
    )

    missing_in_silver = (
        bronze_customers_without_orders
        .join(
            silver_customers.select("c_custkey"),
            on="c_custkey",
            how="left_anti",
        )
    )

    assert missing_in_silver.count() == 0