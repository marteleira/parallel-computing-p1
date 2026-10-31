"""
windows.py
-----------
M3 window functions over the zone/month statistics
-------------------------
"""

import pyspark.sql.functions as sf
from pyspark.sql import Window

from .schema import DEVIATION_STDDEVS, ROLLING_MONTHS


def zone_windows(stats):
    """Add volume rank in borough, rolling trip share in borough and a volume anomaly flag"""

    #range over a month number (a month with no trips is not skipped over)
    zone_months = Window.partitionBy("PULocationID").orderBy("month_index")
    borough_months = Window.partitionBy("pickup_borough").orderBy("month_index")
    rolling = (-(ROLLING_MONTHS - 1), 0)

    history = (-ROLLING_MONTHS, -1) #excludee the current month

    return (stats
            .withColumn("month_index", sf.year("month") * 12 + sf.month("month"))
            .withColumn("volume_rank", sf.rank().over(
                Window.partitionBy("pickup_borough", "month").orderBy(sf.desc("trip_count"))))
            .withColumn("rolling_trip_share",
                        sf.sum("trip_count").over(zone_months.rangeBetween(*rolling))
                        / sf.sum("trip_count").over(borough_months.rangeBetween(*rolling)))
            .withColumn("rolling_mean_trips",
                        sf.mean("trip_count").over(zone_months.rangeBetween(*history)))
            .withColumn("rolling_stddev_trips",
                        sf.stddev("trip_count").over(zone_months.rangeBetween(*history)))
            # null when there is not enough history (less than 2 months)
            .withColumn("volume_anomaly",
                        sf.abs(sf.col("trip_count") - sf.col("rolling_mean_trips"))
                        > DEVIATION_STDDEVS * sf.col("rolling_stddev_trips"))
            .drop("month_index"))
