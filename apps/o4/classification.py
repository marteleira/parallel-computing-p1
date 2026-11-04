"""
classification.py
-----------
O4 trip-type fractions and zone type per pickup zone and hour
-------------------------
"""

import pyspark.sql.functions as sf

from .schema import (AIRPORT_RATECODES, GROUP_MIN_PASSENGERS, NEGOTIATED_RATECODE,
                     ZONE_TYPE_THRESHOLDS)


def classify_trips(df, thresholds=ZONE_TYPE_THRESHOLDS):
    """Return the trips with the fractions and zone_type of their (zone, hour) bucket"""
    trips = df.withColumn("hour_of_day", sf.hour("tpep_pickup_datetime"))

    # count_if counts a null RatecodeID as "not that type"
    trip_count = sf.count(sf.lit(1))
    buckets = (trips.groupBy("PULocationID", "hour_of_day").agg(
                   (sf.count_if(sf.col("RatecodeID").isin(AIRPORT_RATECODES)) / trip_count)
                   .alias("airport_fraction"),
                   (sf.count_if(sf.col("RatecodeID") == NEGOTIATED_RATECODE) / trip_count)
                   .alias("negotiated_fraction"),
                   (sf.count_if(sf.col("passenger_count") >= GROUP_MIN_PASSENGERS) / trip_count)
                   .alias("group_fraction"),
               )
               .withColumn("zone_type", _zone_type(thresholds)))

    # every trip has a bucket (built from the same trips), so the inner join drops nothing
    return trips.join(sf.broadcast(buckets), on=["PULocationID", "hour_of_day"], how="inner")


def _zone_type(thresholds):
    """Column with the first zone type whose fraction reaches its threshold, else 'mixed'."""
    return (sf.when(sf.col("airport_fraction") >= thresholds["transit"], "transit")
            .when(sf.col("negotiated_fraction") >= thresholds["commercial"], "commercial")
            .when(sf.col("group_fraction") >= thresholds["residential"], "residential")
            .otherwise("mixed"))
