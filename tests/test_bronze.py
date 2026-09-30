from pyspark.sql import SparkSession

from config.settings import (
    CUSTOMER_SOURCE,
    ORDERS_SOURCE,
    CUSTOMER_BRONZE,
    ORDERS_BRONZE,
)

spark = SparkSession.getActiveSession()

VALID_SEGMENTS = {
    "BUILDING",
    "MACHINERY",
    "AUTOMOBILE",
    "HOUSEHOLD",
    "FURNITURE",
}

def test_customer_row_count():
    source = spark.table(CUSTOMER_SOURCE)
    bronze = spark.table(CUSTOMER_BRONZE)

    assert source.count() == bronze.count()

def test_order_row_count():
    source = spark.table(ORDERS_SOURCE)
    bronze = spark.table(ORDERS_BRONZE)

    assert source.count() == bronze.count()

def test_customer_schema():
    source = spark.table(CUSTOMER_SOURCE)
    bronze = spark.table(CUSTOMER_BRONZE)

    assert source.schema == bronze.schema

def test_order_schema():
    source = spark.table(ORDERS_SOURCE)
    bronze = spark.table(ORDERS_BRONZE)

    assert source.schema == bronze.schema

def test_customer_key_uniqueness():
    df = spark.table(CUSTOMER_BRONZE)

    total_count = df.count()
    unique_count = df.select("c_custkey").distinct().count()

    assert total_count == unique_count

def test_order_key_uniqueness():
    df = spark.table(ORDERS_BRONZE)

    total_count = df.count()
    unique_count = df.select("o_orderkey").distinct().count()

    assert total_count == unique_count

def test_customer_key_not_null():
    df = spark.table(CUSTOMER_BRONZE)

    null_count = df.filter(df.c_custkey.isNull()).count()

    assert null_count == 0

def test_order_key_not_null():
    df = spark.table(ORDERS_BRONZE)

    null_count = df.filter(df.o_orderkey.isNull()).count()

    assert null_count == 0

def test_customer_market_segment_not_null():
    bronze = spark.table(CUSTOMER_BRONZE)

    assert bronze.filter(
        bronze.c_mktsegment.isNull()
    ).count() == 0

def test_customer_market_segment_valid():
    bronze = spark.table(CUSTOMER_BRONZE)

    invalid_count = (
        bronze
        .filter(
            ~bronze.c_mktsegment.isin(VALID_SEGMENTS)
        )
        .count()
    )

    assert invalid_count == 0

def test_orders_have_valid_customer():
    orders = spark.table(ORDERS_BRONZE)
    customers = spark.table(CUSTOMER_BRONZE)

    invalid_orders = (
        orders
        .join(
            customers,
            orders.o_custkey == customers.c_custkey,
            "left_anti",
        )
    )

    assert invalid_orders.count() == 0

def test_customer_data_matches_source():
    source = spark.table(CUSTOMER_SOURCE)
    bronze = spark.table(CUSTOMER_BRONZE)

    source_only = source.exceptAll(bronze).count()
    bronze_only = bronze.exceptAll(source).count()

    assert source_only == 0
    assert bronze_only == 0

def test_orders_data_matches_source():
    source = spark.table(ORDERS_SOURCE)
    bronze = spark.table(ORDERS_BRONZE)

    source_only = source.exceptAll(bronze).count()
    bronze_only = bronze.exceptAll(source).count()

    assert source_only == 0
    assert bronze_only == 0
