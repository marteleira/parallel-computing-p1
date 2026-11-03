"""
features.py
-----------
O3 bucket mean / variance, 4-week lag self-join, demand index and service zone
-------------------------
"""

import pyspark.sql.functions as sf

import shared
from .schema import BUCKET_COLUMNS, DEMAND_INDEX_THRESHOLD, LAG_DAYS, SLOT_COLUMNS


def demand_features(counts, zones):
    """Return every slot with its bucket mean/variance, 4-week lag, demand index and spike flag"""

    # historical = every date of the loaded period (current slot included), sample variance
    bucket_stats = counts.groupBy(*BUCKET_COLUMNS).agg(
        sf.mean("trip_count").alias("mean_trip_count"),
        sf.variance("trip_count").alias("variance_trip_count"),
    )

    # left join: the first 4 weeks (or an empty slot 4 weeks before) have a null lag
    return (counts
            .join(bucket_stats, on=BUCKET_COLUMNS, how="inner")
            .join(_lagged(counts), on=SLOT_COLUMNS, how="left")
            .withColumn("demand_index", sf.col("trip_count") / sf.col("mean_trip_count"))
            .withColumn("is_demand_spike", sf.col("demand_index") > DEMAND_INDEX_THRESHOLD)
            .join(shared.pickup_zones(zones), on="PULocationID", how="inner"))


def _lagged(counts):
    """The same counts with pickup_date moved LAG_DAYS forward, to self-join onto that date."""
    return counts.select(
        "PULocationID",
        "hour_of_day",
        sf.date_add("pickup_date", LAG_DAYS).alias("pickup_date"),
        sf.col("trip_count").alias("lag_4w_trip_count"),
    )
