"""Event-time sliding-window statistics per device."""

from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from producer.event_schema import NUMERIC_FIELDS


def window_aggregates(events: DataFrame, window: str, slide: str) -> DataFrame:
    """Sliding-window metrics. ``events`` must already carry a watermark.

    In *update* output mode each micro-batch emits only the windows that changed, which
    the sink upserts on (device_id, window_start, window_end), so a window can be
    refined by late events until the watermark closes it.
    """
    missing = reduce(lambda a, b: a + b, [F.col(m).isNull().cast("int") for m in NUMERIC_FIELDS])
    return (
        events.withColumn("missing_count", missing)
        # Group by device only: plant is an attribute. Grouping by plant too produced two rows
        # for one (device, window) when a device was re-assigned mid-window, which broke the
        # upsert key (found by the load test).
        .groupBy(F.window("event_time", window, slide).alias("w"), "device_id")
        .agg(
            F.max("plant_id").alias("plant_id"),
            F.avg("temperature").alias("avg_temperature"),
            F.min("temperature").alias("min_temperature"),
            F.max("temperature").alias("max_temperature"),
            F.stddev_samp("temperature").alias("std_temperature"),
            F.avg("vibration").alias("avg_vibration"),
            F.max("vibration").alias("max_vibration"),
            F.stddev_samp("vibration").alias("std_vibration"),
            F.avg("pressure").alias("avg_pressure"),
            F.min("pressure").alias("min_pressure"),
            F.avg("rpm").alias("avg_rpm"),
            F.avg("power_consumption").alias("avg_power_consumption"),
            F.count(F.lit(1)).alias("event_count"),
            F.sum("missing_count").alias("missing_total"),
        )
        .select(
            "device_id",
            "plant_id",
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "avg_temperature", "min_temperature", "max_temperature", "std_temperature",
            "avg_vibration", "max_vibration", "std_vibration",
            "avg_pressure", "min_pressure", "avg_rpm", "avg_power_consumption",
            "event_count",
            (F.col("missing_total") / (F.col("event_count") * len(NUMERIC_FIELDS))).alias(
                "missing_measurement_rate"
            ),
        )
    )
