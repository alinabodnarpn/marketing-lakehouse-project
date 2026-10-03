from config.settings import CUSTOMER_SILVER, ORDERS_SILVER
def validate_order_customer_references(spark):
    customers = spark.table(CUSTOMER_SILVER)
    orders = spark.table(ORDERS_SILVER)

    invalid_orders = (
        orders
        .join(
            customers,
            orders["o_custkey"] == customers["c_custkey"],
            "left_anti"
        )
    )
    invalid_count = invalid_orders.count()
    if invalid_count > 0:
        raise ValueError(
            f"Validation failed: {invalid_count} orders "
            "reference non-existing customers."
        )
    print("Validation PASSED")
    print("Every order has a valid customer reference.")
    print(f"Invalid customer references: {invalid_count}")
    return invalid_count