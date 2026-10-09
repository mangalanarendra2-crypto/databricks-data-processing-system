"""Lightweight data-quality checks that can fail the pipeline."""
from dataclasses import dataclass
from typing import Callable, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


class DataQualityError(Exception):
    pass


def not_empty(df: DataFrame, name: str) -> CheckResult:
    n = df.count()
    return CheckResult(f"{name}: not empty", n > 0, f"{n} rows")


def no_nulls(df: DataFrame, name: str, columns: List[str]) -> CheckResult:
    cond = F.lit(False)
    for c in columns:
        cond = cond | F.col(c).isNull()
    bad = df.filter(cond).count()
    return CheckResult(f"{name}: no nulls in {columns}", bad == 0, f"{bad} offending rows")


def unique_key(df: DataFrame, name: str, key: str) -> CheckResult:
    dupes = df.groupBy(key).count().filter("count > 1").count()
    return CheckResult(f"{name}: unique {key}", dupes == 0, f"{dupes} duplicated keys")


def positive(df: DataFrame, name: str, column: str) -> CheckResult:
    bad = df.filter(F.col(column) <= 0).count()
    return CheckResult(f"{name}: {column} > 0", bad == 0, f"{bad} offending rows")


def run_checks(checks: List[Callable[[], CheckResult]], fail_on_error: bool = True) -> List[CheckResult]:
    results = [check() for check in checks]
    for r in results:
        print(f"[{'PASS' if r.passed else 'FAIL'}] {r.name} ({r.detail})")
    failed = [r for r in results if not r.passed]
    if failed and fail_on_error:
        raise DataQualityError(f"{len(failed)} data quality check(s) failed")
    return results
