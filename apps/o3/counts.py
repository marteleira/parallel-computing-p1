"""
counts.py
-----------
O3 trip count per pickup zone, date and hour
-------------------------
"""

import pyspark.sql.functions as sf

from .schema import SLOT_COLUMNS


def hourly_counts(df):
    """Return the trip count per (PULocationID, pickup_date, hour_of_day), with its day_of_week"""
    trips = (df.withColumn("pickup_date", sf.to_date("tpep_pickup_datetime"))
             .withColumn("hour_of_day", sf.hour("tpep_pickup_datetime")))

    # an hour with no trips has no row (it is not counted as 0)
    return (trips.groupBy(*SLOT_COLUMNS)
            .agg(sf.count(sf.lit(1)).alias("trip_count"))
            .withColumn("day_of_week", sf.dayofweek("pickup_date")))
