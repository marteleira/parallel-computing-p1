# AGENTS.md

PySpark pipeline over the NYC TLC Yellow Taxi trip records (Project Assignment 1, Large-Scale
Analytics with Spark, deadline 2026-11-06). Read this file fully before touching the code.

## What should be done

- Write the **simplest correct version first** (the baseline). No speculative optimisations:
  no `cache()`, `broadcast()`, `repartition()`, AQE/config tuning, salting, etc. unless the user
  asks for it after profiling. Every optimisation must later have a measured "before".
- Keep the code easy to read and easy to explain in the report. If something needs a long
  comment to understand, simplify it instead.

## Current status

| Op | Status | What |
|----|--------|------|
| M1 | done   | ingestion, schema enforcement, invalid-record filtering, per-rule stats |
| M2 | done   | temporal demand per (pickup_borough, day_of_week, hour_of_day) |
| M3 | done   | zone/month stats + window functions |
| O1 | done   | fare-per-mile anomalies per (zone, hour), rates weekday vs weekend |
| O4 | done   | trip-type fractions per (zone, hour), zone type, stats per zone type/month |
| O3 | todo   | demand features, 4-week lag self-join (after O4) |

Work on one operation at a time, in that order, unless told otherwise.

## Layout

```
apps/
  pipeline.py        # entry point: CLI args, SparkSession, calls each module in order
  m1/                # one package per operation (m1, m2, m3, o1..o4)
    __init__.py      # header docstring + re-exports + __all__ (the public API)
    schema.py        # constants, schemas, rules (no logic)
    ingestion.py     # one file per responsibility
    cleaning.py
  m2/                # aggregations.py: temporal_demand(clean, zones)
  shared/            # code used by several ops (zones.py: load_zones)
data/                # dataset + taxi_zone_lookup.csv, never committed; output/ = Spark results
readme.md            # reproduction steps (required by the assignment)
```

Docker mounts (`../docker-spark-env/docker-compose.yml`): `apps/` → `/apps/parallel-computing-p1`,
`data/` → `/data/parallel-computing-p1`. Spark 4.x, `spark:python3` image, workers with 2 cores / 6G.

## Running

From `../docker-spark-env`, with the cluster up (`docker compose up -d --scale spark-worker=N`):

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /apps/parallel-computing-p1/pipeline.py \
    --data /data/parallel-computing-p1 --years 2019 --months 1 --fraction 0.01 --stats
```

- The assignment scope is **2016–2020**: use `--years 2016-2020` for real runs (the default
  `all` also picks up every other year present in `data/`).
- Smoke test = one month + `--fraction 0.01`. Always run a smoke test after a change and show
  the user the output. Do not launch full-dataset runs without asking (they take long).
- Spark UI: master on http://localhost:8080; the app UI port is printed at startup.

## Module conventions (follow m1 exactly)

**New operation = new package** `apps/<op>/` (`m2`, `m3`, `o1`, ...), same shape as `m1`:

- `__init__.py`: header docstring, imports the public functions, `__all__` listing them.
- Split files by responsibility (e.g. `schema.py` for constants, `aggregations.py`,
  `windows.py`, `output.py`). Few small files beat one big one, but don't create a file for
  a single 3-line function either.
- Constants, thresholds, column lists and rules go in a `schema.py`-like file, at the top,
  in UPPER_CASE. No magic numbers in the logic.
- Code used by more than one operation (e.g. the zone lookup loader) goes in `apps/shared/`,
  same package shape.

**Every `.py` file starts with this header**:

```python
"""
<file>.py
-----------
<OP> <one line saying what this file does>
-------------------------
"""
```

**Inside a file**: imports → constants → public functions → private `_helpers` at the bottom.

**Public API is small**: a module exposes 1–3 functions that take a DataFrame (plus `spark`
only if it must read something) and return a DataFrame. `pipeline.py` only orchestrates:

```python
# M2 —> temporal demand
demand = m2.temporal_demand(clean, zones)
```

No business logic in `pipeline.py`; no argparse or SparkSession creation inside modules.

## Python style

- Plain functions. No classes, no type hints, no dataclasses, no logging module (`print`).
- `import pyspark.sql.functions as sf` (never `F`, never `from ... import *`).
- DataFrame API by default. SQL strings are fine for simple conditions kept in a dict/constant
  (like `INVALID_RULES`). Pick whichever reads better and keep it consistent within a module.
- **No Python UDFs** unless the user explicitly wants one (UDF overhead is a report topic).
- f-strings; printed tables aligned like `m1.rule_stats` (`{name:<42}{n:>15,}`).
- Line length ~100. Short names are fine for locals (`df`, `row`, `n`); be explicit for
  columns (`pickup_borough`, `trip_minutes`).
- Docstrings: one line, says what the function returns/does. Multi-line only when there is a
  non-obvious *why* (see `ingestion._group_by_schema`).
- Comments: short, lowercase-ish, explain **why**, never narrate the code. Don't comment
  the obvious.
- Complexity ceiling: `m1.cleaning.rule_stats` is already the upper limit. If a function gets
  more complicated than that, split it or simplify it.
- Get it done: no tests framework, no config files, no extra dependencies, no abstraction
  "for later". Only what the current operation needs.

## Spark rules for the baseline

- **No hidden actions** inside module functions (`count`, `collect`, `show`, `first`,
  `toPandas`). Actions happen in `pipeline.py` (the write) or behind an explicit flag like
  `--stats`. Hidden actions distort the profiling.
- Each operation ends in **one write** to Parquet through `_write(df, args.output, "<op>")`
  in `pipeline.py`, which lands in `<output>/<op>` and prints the elapsed time of that write
  (= that op's time, since everything before it is lazy). M2 is partitioned by `pickup_borough`.
- The cleaned trips (`<output>/m1`) are only written with `--write-clean`: not required by the
  assignment and large, so they would dominate the baseline.
- Derived columns are computed once and named clearly (`hour_of_day`, `day_of_week`,
  `trip_minutes`, `month`), with `spark.sql.session.timeZone = UTC` already set.
- Zone lookup: `data/taxi_zone_lookup.csv`
  (https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv). Join it as a plain join
  for now (pickup side, and dropoff side when needed); broadcast is an optimisation for later.
- Leave obvious optimisation targets visible (e.g. `clean` reused by M2 and M3 without
  cache). Note them to the user instead of fixing them.

## Operation specs (short)

**M2**: per `(pickup_borough, day_of_week, hour_of_day)`: trip count, mean `fare_amount`,
mean trip duration in minutes, mean `trip_distance`. Write Parquet partitioned by borough.

**M3**: per `(pickup zone, month)`: mean and stddev of `fare_amount` and `trip_distance`, plus
with window functions: rank zones by monthly trip volume within borough; rolling 3-month trip
share within borough; flag zones whose current-month volume deviates from their own rolling
mean by > 1.5 stddev.

**O1** fare anomalies per (PULocationID, hour) using fare/distance, mean + 3σ, weekday vs
weekend by payment_type and borough. **O2** OD flows, route share of outbound trips, top-20
per borough, Q1 vs Q3 share reversal. **O3** demand features, 4-week lag as a self-join,
demand index > 2.0, annotate by service_zone. **O4** trip-type classification by RatecodeID /
passenger fractions with configurable thresholds, joined back to all trips, stats per
zone type per month.

If a spec is ambiguous (e.g. what "month" means across years, which σ), pick the simplest
reasonable reading, write it as a one-line comment, and tell the user.

## Git and data

- Never commit anything under `data/` except `readme.md`. Never commit Spark output.
- Don't commit unless the user asks. One commit per operation / per optimisation, so each
  before/after can be reproduced from history.
- Don't change M1 behaviour without asking (the cleaning numbers are in the report).
