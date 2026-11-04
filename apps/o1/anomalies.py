"""
anomalies.py
-----------
O1 fare-per-mile anomaly detection per pickup zone and hour
-------------------------
"""

import pyspark.sql.functions as sf

from .schema import ANOMALY_STDDEVS


def flag_fare_anomalies(df):
    """Return the trips with fare_per_mile and an is_fare_anomaly flag vs their (zone, hour)"""

    # trip_distance > 0 is ensured by M1 (no division by 0)
    trips = (df.withColumn("fare_per_mile", sf.col("fare_amount") / sf.col("trip_distance"))
             .withColumn("hour_of_day", sf.hour("tpep_pickup_datetime")))

    baseline = trips.groupBy("PULocationID", "hour_of_day").agg(
        sf.mean("fare_per_mile").alias("mean_fare_per_mile"),
        sf.stddev("fare_per_mile").alias("stddev_fare_per_mile"),
    )

    # stats joined back in every trip, null when the (zone, hour) has a only one trip
    return (trips.join(sf.broadcast(baseline), on=["PULocationID", "hour_of_day"], how="inner")
            .withColumn("is_fare_anomaly",
                        sf.col("fare_per_mile") > sf.col("mean_fare_per_mile")
                        + ANOMALY_STDDEVS * sf.col("stddev_fare_per_mile")))
