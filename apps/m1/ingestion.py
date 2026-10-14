"""
ingestion.py
-----------
M1 file discovery, loading and schema enforcement
-------------------------
"""

import glob
import os
import re
import sys

import pyspark.sql.functions as sf

from .schema import CANONICAL_SCHEMA, ZERO_FILL_COLUMNS

_FILE_RE = re.compile(r"yellow_tripdata_(\d{4})-(\d{2})\.parquet$")


def discover_files(paths, years, months):
    """Expand directories / globs and keep the files of the requested years and months."""
    candidates = []
    for path in paths:
        if os.path.isdir(path):
            candidates += glob.glob(os.path.join(path, "*.parquet"))
        else:
            candidates += glob.glob(path)

    years, months = set(years), set(months)
    selected = []
    for f in sorted(set(candidates)):
        m = _FILE_RE.search(os.path.basename(f))
        if m and int(m.group(1)) in years and int(m.group(2)) in months:
            selected.append(os.path.abspath(f))
    return selected


def load(spark, files):
    """Read the files onto CANONICAL_SCHEMA, with surcharge nulls set to 0."""
    df = None
    for group in _usable_groups(spark, files):
        part = _to_canonical(spark.read.parquet(*group))
        df = part if df is None else df.unionByName(part)
    return df.fillna(0.0, subset=ZERO_FILL_COLUMNS)


def _group_by_schema(spark, files):
    """
    Schemas change across years (column case, int vs double), so one
    spark.read.parquet() over all files fails. Group files by physical schema.
    Returns [(column names, files)].
    """
    groups = {}
    for f in files:
        schema = spark.read.parquet(f).schema
        groups.setdefault(schema.json(), (schema.fieldNames(), []))[1].append(f)
    return list(groups.values())


def _usable_groups(spark, files):
    """Drop groups without location IDs (2009-2010, lat/lon only): every row would be invalid."""
    groups = _group_by_schema(spark, files)
    print(f"Schema groups: {len(groups)}")

    usable = []
    for columns, group in groups:
        if {"pulocationid", "dolocationid"} <= {c.lower() for c in columns}:
            usable.append(group)
        else:
            print(f"  skipping {len(group)} file(s) without PULocationID/DOLocationID, "
                  f"e.g. {os.path.basename(group[0])}")

    if not usable:
        sys.exit("No input file has the PULocationID/DOLocationID columns.")
    return usable


def _to_canonical(df):
    """Select / rename / cast the columns of one schema group onto CANONICAL_SCHEMA."""
    by_lower = {c.lower(): c for c in df.columns}
    cols = []
    for field in CANONICAL_SCHEMA.fields:
        src = by_lower.get(field.name.lower())
        col = sf.lit(None) if src is None else sf.col(src)
        cols.append(col.cast(field.dataType).alias(field.name))
    return df.select(cols)
