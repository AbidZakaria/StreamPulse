"""Record validation as Spark column expressions.

Mirrors :func:`producer.event_schema.validate_event` rule-for-rule (the unit tests run
the same fixtures through both). Every rejected record gets exactly one reason, the
first rule it fails, in this order:

1. ``malformed_json``          payload is not a JSON object
2. ``missing_required_field``  event_id / device_id / event_time missing or blank
3. ``invalid_event_time``      event_time is not an ISO-8601 timestamp
4. ``non_numeric_measurement`` a measurement is present but not a number
5. ``out_of_physical_range``   a measurement is physically impossible
6. ``no_measurements``         every measurement is null

Spark 4 runs with ANSI mode on, where ``CAST('abc' AS DOUBLE)`` raises. Only
``try_cast`` is used here, so bad input becomes NULL and is classified instead of
failing the micro-batch.
"""

from __future__ import annotations

from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from producer.event_schema import (
    NUMERIC_FIELDS,
    PHYSICAL_RANGES,
    REASON_BAD_TIMESTAMP,
    REASON_BAD_TYPE,
    REASON_MALFORMED_JSON,
    REASON_MISSING_FIELD,
    REASON_NO_MEASUREMENTS,
    REASON_OUT_OF_RANGE,
    REQUIRED_FIELDS,
)


def _blank(c: Column) -> Column:
    return c.isNull() | (F.trim(c) == "")


def _any(conditions: list[Column]) -> Column:
    return reduce(lambda a, b: a | b, conditions)


def _all(conditions: list[Column]) -> Column:
    return reduce(lambda a, b: a & b, conditions)


def num(field: str) -> Column:
    """Typed measurement column (NULL when missing or not numeric)."""
    return F.col(f"j.{field}").try_cast("double")


def with_reject_reason(parsed: DataFrame) -> DataFrame:
    """Add ``reject_reason`` (NULL for valid records) to the output of ``parse_raw``.

    Expects a struct column ``j`` holding the payload parsed with every field as STRING,
    plus ``j._corrupt_record``.
    """
    malformed = F.col("j").isNull() | F.col("j._corrupt_record").isNotNull()
    missing = _any([_blank(F.col(f"j.{f}")) for f in REQUIRED_FIELDS])
    bad_time = F.col("j.event_time").try_cast("timestamp").isNull()
    bad_type = _any([F.col(f"j.{f}").isNotNull() & num(f).isNull() for f in NUMERIC_FIELDS])
    out_of_range = _any(
        [
            num(f).isNotNull() & ~num(f).between(F.lit(lo), F.lit(hi))
            for f, (lo, hi) in PHYSICAL_RANGES.items()
        ]
    )
    no_measurements = _all([F.col(f"j.{f}").isNull() for f in NUMERIC_FIELDS])

    reason = (
        F.when(malformed, F.lit(REASON_MALFORMED_JSON))
        .when(missing, F.lit(REASON_MISSING_FIELD))
        .when(bad_time, F.lit(REASON_BAD_TIMESTAMP))
        .when(bad_type, F.lit(REASON_BAD_TYPE))
        .when(out_of_range, F.lit(REASON_OUT_OF_RANGE))
        .when(no_measurements, F.lit(REASON_NO_MEASUREMENTS))
    )
    return parsed.withColumn("reject_reason", reason)
