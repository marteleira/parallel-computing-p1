"""
aggregations.py
-----------
M3 per zone and month fare / distance statistics
-------------------------
"""

import pyspark.sql.functions as sf

import shared


def zone_month_stats(df, zones):
    """Returns the trip count, mean and stardard-deviation of fare and distance per zone and month"""

    # month is the calendar month of the pickup (changes between year)
    enriched = (df.join(shared.pickup_zones(zones), on="PULocationID", how="inner")
                .withColumn("month", sf.trunc("tpep_pickup_datetime", "month")))
    
    return enriched.groupBy("pickup_borough", "PULocationID", "pickup_zone", "month").agg(
        sf.count(sf.lit(1)).alias("trip_count"),
        sf.mean("fare_amount").alias("mean_fare_amount"),
        sf.stddev("fare_amount").alias("stddev_fare_amount"), # stardard-dev is null for a zone with a single trip in that month
        sf.mean("trip_distance").alias("mean_trip_distance"),
        sf.stddev("trip_distance").alias("stddev_trip_distance"),
    )
