"""Silver layer: type casting, standardisation, validation and de-duplication."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

VALID_STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]


def _dedupe(df: DataFrame, key: str) -> DataFrame:
    """Keep the latest ingested record per business key."""
    w = Window.partitionBy(key).orderBy(F.col("_ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def clean_customers(df: DataFrame) -> DataFrame:
    cleaned = (
        df.withColumn("customer_id", F.upper(F.trim("customer_id")))
        .withColumn("name", F.initcap(F.trim("name")))
        .withColumn("email", F.lower(F.trim("email")))
        .withColumn("city", F.initcap(F.trim("city")))
        .withColumn("signup_date", F.to_date("signup_date", "yyyy-MM-dd"))
        .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") != ""))
        # Basic email validity: keep row but null out malformed addresses
        .withColumn(
            "email",
            F.when(F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"), F.col("email")),
        )
    )
    return _dedupe(cleaned, "customer_id")


def clean_orders(df: DataFrame) -> DataFrame:
    cleaned = (
        df.withColumn("order_id", F.col("order_id").cast("int"))
        .withColumn("customer_id", F.upper(F.trim("customer_id")))
        .withColumn("product", F.initcap(F.trim("product")))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("status", F.upper(F.trim("status")))
        .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
        .dropna(subset=["order_id", "customer_id", "quantity", "unit_price", "order_date"])
        .filter((F.col("quantity") > 0) & (F.col("unit_price") > 0))
        .filter(F.col("status").isin(VALID_STATUSES))
        .withColumn("total_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
    )
    return _dedupe(cleaned, "order_id")
