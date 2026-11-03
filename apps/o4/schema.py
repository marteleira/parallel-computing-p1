"""
schema.py
-----------
O4 trip-type definitions and zone type thresholds
-------------------------
"""

AIRPORT_RATECODES = [2, 3]  # 2 = JFK, 3 = Newark
NEGOTIATED_RATECODE = 5
GROUP_MIN_PASSENGERS = 4

# zone type -> minimum fraction of its trip type in the (zone, hour) bucket, set near the
# 90th percentile of each fraction over the buckets of 2019-01 (top ~10% get the label).
# checked in this order (first match wins), a bucket that matches none is "mixed"
ZONE_TYPE_THRESHOLDS = {
    "transit": 0.10,      # airport_fraction
    "commercial": 0.35,   # negotiated_fraction
    "residential": 0.12,  # group_fraction
}
