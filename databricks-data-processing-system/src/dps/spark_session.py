from pyspark.sql import SparkSession


def get_spark(app_name: str = "dps") -> SparkSession:
    """Return the active session on Databricks, or a local session elsewhere."""
    spark = SparkSession.getActiveSession()
    if spark is not None:
        return spark
    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
