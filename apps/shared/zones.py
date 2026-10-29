"""
zones.py
-----------
Shared taxi zone lookup loader
-------------------------
"""

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# Explicit schema, inferSchema would cost an extra job over the CSV
ZONES_SCHEMA = StructType([
    StructField("LocationID", IntegerType()),
    StructField("Borough", StringType()),
    StructField("Zone", StringType()),
    StructField("service_zone", StringType()),
])


def load_zones(spark, path):
    """Read taxi_zone_lookup.csv (LocationID -> Borough, Zone, service_zone)."""
    return spark.read.csv(path, header=True, schema=ZONES_SCHEMA)
