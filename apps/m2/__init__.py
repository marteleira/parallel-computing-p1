"""
m2
-----------
M2 temporal demand analysis
-------------------------
"""

from .aggregations import temporal_demand

# Public API, everything else (and every "_name") is internal
__all__ = ["temporal_demand"]
