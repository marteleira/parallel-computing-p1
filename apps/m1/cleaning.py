"""
cleaning.py
-----------
M1 invalid-record filtering
-------------------------
"""

import pyspark.sql.functions as sf

from .schema import INVALID_RULES


def filter_invalid(df):
    """Keep only the records that pass every rule"""
    return df.filter(~_is_invalid())


def rule_stats(df):
    """One pass -> print rows rejected per rule (rules overlap) and return the valid count"""
    aggs = [sf.count(sf.lit(1)).alias("total")]
    aggs += [sf.sum(sf.when(sf.expr(rule), 1).otherwise(0)).alias(f"r{i}")
             for i, rule in enumerate(INVALID_RULES.values())]
    aggs.append(sf.sum(sf.when(_is_invalid(), 0).otherwise(1)).alias("valid"))
    row = df.agg(*aggs).first()

    total = row["total"] or 0
    print(f"\n{'rule':<42}{'rejected':>15}{'%':>9}")
    for i, name in enumerate(INVALID_RULES):
        n = row[f"r{i}"] or 0
        print(f"{name:<42}{n:>15,}{100 * n / max(total, 1):>8.2f}%")
    valid = row["valid"] or 0
    print(f"{'total records':<42}{total:>15,}")
    print(f"{'valid records':<42}{valid:>15,}{100 * valid / max(total, 1):>8.2f}%\n")
    return valid


def _is_invalid():
    return sf.expr(" OR ".join(f"({rule})" for rule in INVALID_RULES.values()))
