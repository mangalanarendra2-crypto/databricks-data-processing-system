"""Explicit schemas: never rely on inferSchema in production pipelines."""
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

# Raw/bronze schemas are all strings so nothing is lost on read; typing happens in silver.
CUSTOMERS_RAW = StructType(
    [
        StructField("customer_id", StringType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("city", StringType()),
        StructField("signup_date", StringType()),
    ]
)

ORDERS_RAW = StructType(
    [
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("product", StringType()),
        StructField("quantity", StringType()),
        StructField("unit_price", StringType()),
        StructField("status", StringType()),
        StructField("order_date", StringType()),
    ]
)
