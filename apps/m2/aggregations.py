"""
aggregations.py
-----------
M2 temporal demand aggregation
-------------------------
"""

import pyspark.sql.functions as sf

import shared


def temporal_demand(df, zones):
    """Return trip demand aggregated by pickup borough, weekday and hour."""
    #dayofweek: 1 = Sunday,  2 = Monday... etc
    enriched = (df.join(shared.pickup_zones(zones), on="PULocationID", how="inner")
                .withColumn("day_of_week", sf.dayofweek("tpep_pickup_datetime"))
                .withColumn("hour_of_day", sf.hour("tpep_pickup_datetime"))
                .withColumn(
                    "trip_minutes",
                    (sf.unix_timestamp("tpep_dropoff_datetime")
                     - sf.unix_timestamp("tpep_pickup_datetime")) / 60.0,
                ))
    return enriched.groupBy("pickup_borough", "day_of_week", "hour_of_day").agg(
        sf.count(sf.lit(1)).alias("trip_count"),
        sf.mean("fare_amount").alias("mean_fare_amount"),
        sf.mean("trip_minutes").alias("mean_trip_minutes"),
        sf.mean("trip_distance").alias("mean_trip_distance"),
    )
