from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import (
    CUSTOMER_GOLD,
    CUSTOMER_GOLD_SOURCE,
    ORDERS_GOLD_SOURCE,
    RPR_GOLD,
)

spark = SparkSession.getActiveSession()


def test_every_source_customer_in_gold():
    source = spark.table(CUSTOMER_GOLD_SOURCE)
    gold = spark.table(CUSTOMER_GOLD)

    missing = (
        source.select(F.col("c_custkey").alias("customer_key"))
        .join(gold, "customer_key", "left_anti")
        .count()
    )
    assert missing == 0


def test_gold_row_count_matches_source():
    assert spark.table(CUSTOMER_GOLD_SOURCE).count() == spark.table(CUSTOMER_GOLD).count()


def test_zero_order_customers_preserved():
    source_zero = (
        spark.table(CUSTOMER_GOLD_SOURCE)
        .alias("c")
        .join(
            spark.table(ORDERS_GOLD_SOURCE).alias("o"),
            F.col("c.c_custkey") == F.col("o.o_custkey"),
            "left",
        )
        .groupBy("c.c_custkey")
        .agg(F.count("o.o_orderkey").alias("orders"))
        .filter(F.col("orders") == 0)
        .select(F.col("c_custkey").alias("customer_key"))
    )

    bad = (
        source_zero.join(spark.table(CUSTOMER_GOLD), "customer_key", "left")
        .filter(
            F.col("order_count").isNull() | (F.col("order_count") != 0)
        )
        .count()
    )
    assert bad == 0


def test_no_duplicate_customer_keys():
    gold = spark.table(CUSTOMER_GOLD)
    assert gold.count() == gold.select("customer_key").distinct().count()


def test_no_impossible_metric_values():
    bad = (
        spark.table(CUSTOMER_GOLD)
        .filter(
            (F.col("order_count") < 0)
            | ((F.col("is_activated") == True) & (F.col("order_count") < 1))
            | ((F.col("is_activated") == False) & (F.col("order_count") != 0))
            | ((F.col("is_repeat_customer") == True) & (F.col("order_count") <= 1))
            | ((F.col("is_repeat_customer") == False) & (F.col("order_count") > 1))
        )
        .count()
    )
    assert bad == 0


def test_rpr_between_zero_and_one():
    bad = (
        spark.table(RPR_GOLD)
        .filter(
            (F.col("repeat_purchase_rate") < 0)
            | (F.col("repeat_purchase_rate") > 1)
        )
        .count()
    )
    assert bad == 0
