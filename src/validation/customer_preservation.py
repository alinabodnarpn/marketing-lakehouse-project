from config.settings import (
    CUSTOMER_BRONZE,
    CUSTOMER_SILVER,
    ORDERS_SILVER,
)


def validate_customer_preservation(spark):
    bronze_customers = spark.table(CUSTOMER_BRONZE)
    silver_customers = spark.table(CUSTOMER_SILVER)
    silver_orders = spark.table(ORDERS_SILVER)

    # Check that all Bronze customer IDs are present in Silver
    missing_customers = (
        bronze_customers
        .select("c_custkey")
        .join(
            silver_customers.select("c_custkey"),
            on="c_custkey",
            how="left_anti"
        )
    )

    missing_count = missing_customers.count()

    # Find Silver customers that have no orders
    customers_without_orders = (
        silver_customers
        .select("c_custkey")
        .join(
            silver_orders.select("o_custkey"),
            silver_customers["c_custkey"] == silver_orders["o_custkey"],
            "left_anti"
        )
    )

    customers_without_orders_count = customers_without_orders.count()

    print(f"Customers missing from Silver: {missing_count}")
    print(
        f"Customers without orders preserved in Silver: "
        f"{customers_without_orders_count}"
    )

    if missing_count > 0:
        raise ValueError(
            f"Validation failed: {missing_count} Bronze customers "
            "are missing from Silver."
        )

    print("Validation PASSED")
    print("All Bronze customers are preserved in Silver.")

    return customers_without_orders_count