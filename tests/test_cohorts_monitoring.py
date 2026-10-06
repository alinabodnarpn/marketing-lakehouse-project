from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import (
    ACTIVATION_MONITORING_GOLD,
    COHORT_RETENTION_GOLD,
    CUSTOMER_GOLD,
    NEW_CUSTOMERS_MONITORING_GOLD,
    ORDERS_GOLD_SOURCE,
)
from src.analysis.new_customers import calculate_new_customers

spark = SparkSession.getActiveSession()


def _last_monitored_month_end():
    return (
        spark.table(NEW_CUSTOMERS_MONITORING_GOLD)
        .agg(F.last_day(F.max("month")))
        .collect()[0][0]
    )


# --- Cohort retention (Q4) ---


def test_cohort_has_all_1996_months():
    cohorts = spark.table(COHORT_RETENTION_GOLD)
    assert cohorts.count() == 12
    assert cohorts.select("cohort").distinct().count() == 12
    assert cohorts.filter(F.year("cohort_month") != 1996).count() == 0


def test_cohort_retention_bounds():
    bad = (
        spark.table(COHORT_RETENTION_GOLD)
        .filter(
            (F.col("retention_rate") < 0)
            | (F.col("retention_rate") > 1)
            | (F.col("returned_customers") > F.col("cohort_customers"))
        )
        .count()
    )
    assert bad == 0


def test_cohort_windows_complete():
    assert spark.table(COHORT_RETENTION_GOLD).filter(~F.col("is_window_complete")).count() == 0


def test_cohort_sizes_match_gold():
    gold_cohorts = (
        spark.table(CUSTOMER_GOLD)
        .filter(F.year("first_order_date") == 1996)
        .groupBy(F.trunc("first_order_date", "month").alias("cohort_month"))
        .agg(F.count("*").alias("gold_customers"))
    )
    mismatched = (
        spark.table(COHORT_RETENTION_GOLD)
        .join(gold_cohorts, "cohort_month", "full_outer")
        .filter(
            F.col("cohort_customers").isNull()
            | F.col("gold_customers").isNull()
            | (F.col("cohort_customers") != F.col("gold_customers"))
        )
        .count()
    )
    assert mismatched == 0


def test_q1_cohorts_match_new_customers_answer():
    q1_cohorts = (
        spark.table(COHORT_RETENTION_GOLD)
        .filter(F.col("cohort").isin("1996-01", "1996-02", "1996-03"))
        .agg(F.sum("cohort_customers"))
        .collect()[0][0]
    )
    new_customers = calculate_new_customers(spark).collect()[0]["new_customers"]
    assert q1_cohorts == new_customers


# --- Monitoring ---


def test_monitoring_months_unique():
    new = spark.table(NEW_CUSTOMERS_MONITORING_GOLD)
    assert new.count() == new.select("month").distinct().count()

    activation = spark.table(ACTIVATION_MONITORING_GOLD)
    assert (
        activation.count()
        == activation.select("month", "market_segment").distinct().count()
    )


def test_monitoring_has_no_partial_month():
    data_end = spark.table(ORDERS_GOLD_SOURCE).agg(F.max("o_orderdate")).collect()[0][0]
    assert _last_monitored_month_end() <= data_end
    for table in (NEW_CUSTOMERS_MONITORING_GOLD, ACTIVATION_MONITORING_GOLD):
        months = spark.table(table).select("month").distinct()
        assert months.filter(F.dayofmonth("month") != 1).count() == 0


def test_new_customers_total_matches_gold():
    month_end = _last_monitored_month_end()
    monitored = spark.table(NEW_CUSTOMERS_MONITORING_GOLD).agg(F.sum("new_customers")).collect()[0][0]
    gold = (
        spark.table(CUSTOMER_GOLD)
        .filter(F.col("first_order_date") <= F.lit(month_end))
        .count()
    )
    assert monitored == gold


def test_activation_rate_bounds():
    bad = (
        spark.table(ACTIVATION_MONITORING_GOLD)
        .filter((F.col("activation_rate") < 0) | (F.col("activation_rate") > 1))
        .count()
    )
    assert bad == 0


def test_latest_activation_matches_gold():
    month_end = _last_monitored_month_end()
    latest = (
        spark.table(ACTIVATION_MONITORING_GOLD)
        .filter(F.last_day("month") == F.lit(month_end))
        .select("market_segment", "activated_customers", "total_customers")
    )
    gold = (
        spark.table(CUSTOMER_GOLD)
        .groupBy("market_segment")
        .agg(
            F.count("*").alias("gold_total"),
            F.sum(
                F.when(F.col("first_order_date") <= F.lit(month_end), 1).otherwise(0)
            ).alias("gold_activated"),
        )
    )
    mismatched = (
        latest.join(gold, "market_segment", "full_outer")
        .filter(
            F.col("activated_customers").isNull()
            | F.col("gold_activated").isNull()
            | (F.col("activated_customers") != F.col("gold_activated"))
            | (F.col("total_customers") != F.col("gold_total"))
        )
        .count()
    )
    assert mismatched == 0


def test_no_activation_alerts_on_clean_data():
    assert spark.table(ACTIVATION_MONITORING_GOLD).filter("is_alert").count() == 0
