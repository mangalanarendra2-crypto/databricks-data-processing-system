"""End-to-end pipeline: raw CSV -> bronze -> silver -> gold.

Run locally:        python -m dps.pipeline          (with PYTHONPATH=src)
Run on Databricks:  via notebooks/run_pipeline.py or the job in databricks.yml
"""
from pyspark.sql import DataFrame, SparkSession

from . import cleaning, quality, schemas, transformations
from .config import Config
from .ingestion import ingest
from .spark_session import get_spark


def _write(df: DataFrame, path: str, fmt: str) -> None:
    df.write.format(fmt).mode("overwrite").save(path)


def run(spark: SparkSession, cfg: Config) -> dict:
    fmt = cfg.output_format

    # ---- Bronze ---------------------------------------------------------
    customers_b = ingest(spark, cfg.raw_path, schemas.CUSTOMERS_RAW, pattern="customers*.csv")
    orders_b = ingest(spark, cfg.raw_path, schemas.ORDERS_RAW, pattern="orders*.csv")
    _write(customers_b, f"{cfg.bronze_path}/customers", fmt)
    _write(orders_b, f"{cfg.bronze_path}/orders", fmt)

    # ---- Silver ---------------------------------------------------------
    customers_s = cleaning.clean_customers(spark.read.format(fmt).load(f"{cfg.bronze_path}/customers"))
    orders_s = cleaning.clean_orders(spark.read.format(fmt).load(f"{cfg.bronze_path}/orders"))

    quality.run_checks(
        [
            lambda: quality.not_empty(customers_s, "silver.customers"),
            lambda: quality.unique_key(customers_s, "silver.customers", "customer_id"),
            lambda: quality.not_empty(orders_s, "silver.orders"),
            lambda: quality.unique_key(orders_s, "silver.orders", "order_id"),
            lambda: quality.no_nulls(orders_s, "silver.orders", ["order_id", "customer_id", "order_date"]),
            lambda: quality.positive(orders_s, "silver.orders", "total_amount"),
        ]
    )
    _write(customers_s, f"{cfg.silver_path}/customers", fmt)
    _write(orders_s, f"{cfg.silver_path}/orders", fmt)

    # ---- Gold -----------------------------------------------------------
    facts = transformations.build_order_facts(orders_s, customers_s)
    outputs = {
        "order_facts": facts,
        "daily_revenue": transformations.daily_revenue(facts),
        "revenue_by_product": transformations.revenue_by_product(facts),
        "customer_summary": transformations.customer_summary(facts),
    }
    for name, df in outputs.items():
        _write(df, f"{cfg.gold_path}/{name}", fmt)

    return {name: df.count() for name, df in outputs.items()}


def main() -> None:
    spark = get_spark("dps-pipeline")
    cfg = Config.from_env()
    print(f"Running with config: {cfg}")
    counts = run(spark, cfg)
    print("Gold table row counts:")
    for name, n in counts.items():
        print(f"  {name}: {n}")


if __name__ == "__main__":
    main()
