"""
summary.py
-----------
O1 anomaly rates workingdays compared with weekend, by payment type and pickup borough
-------------------------
"""

import pyspark.sql.functions as sf

import shared
from .schema import WEEKEND_DAYS


def anomaly_summary(flagged, zones):
    """Return trip count, anomaly count and anomaly rate per day type, payment type and pickuppborough"""
    enriched = (flagged.join(shared.pickup_zones(zones), on="PULocationID", how="inner")
                .withColumn("day_type",
                            sf.when(sf.dayofweek("tpep_pickup_datetime").isin(WEEKEND_DAYS),"weekend").otherwise("weekday")))

    return (enriched.groupBy("day_type", "payment_type", "pickup_borough").agg(
                sf.count(sf.lit(1)).alias("trip_count"),
                sf.count_if("is_fare_anomaly").alias("anomaly_count"),
                sf.mean("fare_per_mile").alias("mean_fare_per_mile"),
            )
            .withColumn("anomaly_rate", sf.col("anomaly_count") / sf.col("trip_count")))
