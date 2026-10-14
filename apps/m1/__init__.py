"""
m1
-----------
M1 — Data ingestion and cleaning
-------------------------
"""

from .cleaning import filter_invalid, rule_stats
from .ingestion import discover_files, load

# Public API, everything else (and every "_name") is internal
__all__ = ["discover_files", "load", "filter_invalid", "rule_stats"]
