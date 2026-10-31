"""
m3
-----------
M3 zone-level statistics and window functions
-------------------------
"""

from .aggregations import zone_month_stats
from .windows import zone_windows

#everything else must be internal:
__all__ = ["zone_month_stats", "zone_windows"]
