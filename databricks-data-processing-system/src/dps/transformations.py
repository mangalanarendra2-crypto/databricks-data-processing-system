"""Gold layer: business-ready datasets built from clean silver tables."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_order_facts(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """Orders enriched with customer attributes. Cancelled orders are excluded."""
    return (
        orders.filter(F.col("status") != "CANCELLED")
        .join(
            customers.select("customer_id", "name", "city"),
            on="customer_id",
            how="left",
        )
        .withColumnRenamed("name", "customer_name")
        .select(
            "order_id",
            "order_date",
            "customer_id",
            "customer_name",
            "city",
            "product",
            "quantity",
            "unit_price",
            "total_amount",
            "status",
        )
    )


def daily_revenue(facts: DataFrame) -> DataFrame:
    return (
        facts.groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("total_amount"), 2).alias("revenue"),
        )
        .orderBy("order_date")
    )


def revenue_by_product(facts: DataFrame) -> DataFrame:
    return (
        facts.groupBy("product")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("total_amount"), 2).alias("revenue"),
        )
        .orderBy(F.desc("revenue"))
    )


def customer_summary(facts: DataFrame) -> DataFrame:
    return (
        facts.groupBy("customer_id", "customer_name", "city")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.round(F.sum("total_amount"), 2).alias("lifetime_value"),
            F.max("order_date").alias("last_order_date"),
        )
        .orderBy(F.desc("lifetime_value"))
    )
