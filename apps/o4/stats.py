"""
stats.py
-----------
O4 fare and distance statistics per zone type and month
-------------------------
"""

import pyspark.sql.functions as sf


def zone_type_stats(typed):
    """Return trip count, mean and stddev of fare and distance per zone type and month"""

    # month is the calendar month of the pickup (same as M3)
    trips = typed.withColumn("month", sf.trunc("tpep_pickup_datetime", "month"))

    return trips.groupBy("zone_type", "month").agg(
        sf.count(sf.lit(1)).alias("trip_count"),
        sf.mean("fare_amount").alias("mean_fare_amount"),
        sf.stddev("fare_amount").alias("stddev_fare_amount"),
        sf.mean("trip_distance").alias("mean_trip_distance"),
        sf.stddev("trip_distance").alias("stddev_trip_distance"),
    )
