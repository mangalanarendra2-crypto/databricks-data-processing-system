"""Bronze layer: read raw CSVs as-is and stamp audit columns."""
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def read_raw_csv(
    spark: SparkSession, path: str, schema: StructType, pattern: Optional[str] = None
) -> DataFrame:
    """Read CSV file(s) from `path`. `pattern` filters files in a directory, e.g. 'orders*.csv'."""
    reader = spark.read.option("header", True).option("mode", "PERMISSIVE").schema(schema)
    if pattern:
        reader = reader.option("pathGlobFilter", pattern)
    return reader.csv(path)


def add_audit_columns(df: DataFrame) -> DataFrame:
    return df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
        "_source_file", F.input_file_name()
    )


def ingest(
    spark: SparkSession, path: str, schema: StructType, pattern: Optional[str] = None
) -> DataFrame:
    return add_audit_columns(read_raw_csv(spark, path, schema, pattern))
