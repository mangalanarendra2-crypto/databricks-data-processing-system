import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dps import cleaning, schemas, transformations  # noqa: E402
from dps.config import Config  # noqa: E402
from dps.ingestion import ingest  # noqa: E402
from dps.pipeline import run  # noqa: E402
from dps.spark_session import get_spark  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = get_spark("dps-tests")
    yield s
    s.stop()


def test_clean_orders_drops_bad_rows(spark):
    raw = ingest(spark, str(ROOT / "data/raw/orders.csv"), schemas.ORDERS_RAW)
    clean = cleaning.clean_orders(raw)
    ids = sorted(r.order_id for r in clean.collect())
    # 5007 de-duplicated, 5008 (null qty), 5009 (negative price), 5011 (invalid status) removed
    assert ids == [5001, 5002, 5003, 5004, 5005, 5006, 5007, 5010, 5012]


def test_clean_customers_dedupes_and_validates_email(spark):
    raw = ingest(spark, str(ROOT / "data/raw/customers.csv"), schemas.CUSTOMERS_RAW)
    clean = cleaning.clean_customers(raw)
    rows = {r.customer_id: r for r in clean.collect()}
    assert len(rows) == 5
    assert rows["C001"].name == "Asha Rao"
    assert rows["C001"].email == "asha.rao@example.com"
    assert rows["C003"].email is None  # malformed email


def test_gold_excludes_cancelled_orders(spark):
    orders = cleaning.clean_orders(ingest(spark, str(ROOT / "data/raw/orders.csv"), schemas.ORDERS_RAW))
    customers = cleaning.clean_customers(ingest(spark, str(ROOT / "data/raw/customers.csv"), schemas.CUSTOMERS_RAW))
    facts = transformations.build_order_facts(orders, customers)
    assert facts.filter("status = 'CANCELLED'").count() == 0
    assert facts.count() == 8


def test_full_pipeline_runs(spark, tmp_path):
    cfg = Config(
        raw_path=str(ROOT / "data/raw"),
        bronze_path=str(tmp_path / "bronze"),
        silver_path=str(tmp_path / "silver"),
        gold_path=str(tmp_path / "gold"),
        output_format="parquet",
    )
    counts = run(spark, cfg)
    assert counts["order_facts"] == 8
    assert counts["daily_revenue"] == 6
    assert all(n > 0 for n in counts.values())
