"""
schema.py
-----------
M3 window sizes and anomaly threshold
-------------------------
"""

#Rolling trip share covers the current month and the previous ROLLING_MONTHS - 1
ROLLING_MONTHS = 3

# Flag the zone when |volume - rolling mean| > DEVIATION_STDDEVS * rolling stardarddeviation
DEVIATION_STDDEVS = 1.5
