"""
schema.py
-----------
M1 canonical schema and invalid-record rules
-------------------------
"""

from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

MIN_LOCATION_ID, MAX_LOCATION_ID = 1, 263
MIN_PASSENGERS, MAX_PASSENGERS = 1, 6

# Every file is mapped onto this schema, whatever year it comes from
CANONICAL_SCHEMA = StructType([
    StructField("VendorID", IntegerType()),
    StructField("tpep_pickup_datetime", TimestampType()),
    StructField("tpep_dropoff_datetime", TimestampType()),
    StructField("passenger_count", IntegerType()),
    StructField("trip_distance", DoubleType()),
    StructField("RatecodeID", IntegerType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("payment_type", IntegerType()),
    StructField("fare_amount", DoubleType()),
    StructField("extra", DoubleType()),
    StructField("mta_tax", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("tolls_amount", DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("congestion_surcharge", DoubleType()),
    StructField("airport_fee", DoubleType()),
    StructField("cbd_congestion_fee", DoubleType()),
])

#the only ones to keep in the cache
ANALYSIS_COLUMNS = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
                    "trip_distance", "RatecodeID", "PULocationID", "payment_type", "fare_amount"]

# Surcharges that did not exist in earlier years, null means "not charged"
ZERO_FILL_COLUMNS = ["extra", "mta_tax", "tip_amount", "tolls_amount",
                     "improvement_surcharge", "congestion_surcharge",
                     "airport_fee", "cbd_congestion_fee"]

# name -> SQL condition that is TRUE when the record is invalid (nulls count as invalid)
INVALID_RULES = {
    "fare_amount <= 0":
        "fare_amount IS NULL OR fare_amount <= 0",
    "trip_distance <= 0":
        "trip_distance IS NULL OR trip_distance <= 0",
    "passenger_count = 0 or > 6":
        f"passenger_count IS NULL "
        f"OR passenger_count NOT BETWEEN {MIN_PASSENGERS} AND {MAX_PASSENGERS}",
    "dropoff <= pickup":
        "tpep_pickup_datetime IS NULL OR tpep_dropoff_datetime IS NULL "
        "OR tpep_dropoff_datetime <= tpep_pickup_datetime",
    f"PU/DOLocationID outside {MIN_LOCATION_ID}-{MAX_LOCATION_ID}":
        f"PULocationID IS NULL OR DOLocationID IS NULL "
        f"OR PULocationID NOT BETWEEN {MIN_LOCATION_ID} AND {MAX_LOCATION_ID} "
        f"OR DOLocationID NOT BETWEEN {MIN_LOCATION_ID} AND {MAX_LOCATION_ID}",
}
