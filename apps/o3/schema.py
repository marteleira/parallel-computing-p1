"""
schema.py
-----------
O3 bucket and slot keys, lag offset and demand index threshold
-------------------------
"""

# one hour of one date in one pickup zone (one row of the hourly counts)
SLOT_COLUMNS = ["PULocationID", "pickup_date", "hour_of_day"]

# the historical bucket every slot belongs to
BUCKET_COLUMNS = ["PULocationID", "day_of_week", "hour_of_day"]

# 4 weeks, a multiple of 7 so the lagged slot has the same day_of_week
LAG_DAYS = 28

# a slot is a demand spike when trip_count / bucket mean > this
DEMAND_INDEX_THRESHOLD = 2.0
