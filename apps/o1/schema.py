"""
schema.py
-----------
O1 anomaly threshold and weekend definition
-------------------------
"""

# A trip is an anomaly when fare_per_mile > mean + ANOMALY_STDDEVS * stddeviation of its (zone, hour)
ANOMALY_STDDEVS = 3.0

# dayofweek 1 = Sunday, 7 = Saturday
WEEKEND_DAYS = [1, 7]
