"""
pipeline.py
-----------
Base NYC TLC Yellow Taxi pipeline
-------------------------
"""

import argparse
import os
import sys
import time
from datetime import datetime

from pyspark.sql import SparkSession

import m1
import m2
import m3
import o1
import shared


def main():
    args = _build_arg_parser().parse_args()
    years = _parse_range(args.years, 2009, datetime.now().year, "years")
    months = _parse_range(args.months, 1, 12, "months")

    files = m1.discover_files(args.data, years, months)
    if not files:
        sys.exit(f"No yellow_tripdata_YYYY-MM.parquet files found in {args.data} "
                 f"for years={args.years} months={args.months}")

    spark = _create_spark()
    print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
    print(f"Default parallelism: {spark.sparkContext.defaultParallelism}")
    print(f"Input files: {len(files)} ({os.path.basename(files[0])} .. "
          f"{os.path.basename(files[-1])}), fraction={args.fraction}")

    start = time.perf_counter()

    # M1 —> ingestion and cleaning
    df = m1.load(spark, files)
    if args.fraction < 1.0:
        df = df.sample(fraction=args.fraction, seed=args.seed)
    if args.partitions:
        df = df.repartition(args.partitions)
    if args.stats:
        m1.rule_stats(df)
    clean = m1.filter_invalid(df)
    if args.write_clean:
        _write(clean, args.output, "m1")

    # M2 —> temporal demand
    zones = shared.load_zones(spark, args.zones)
    demand = m2.temporal_demand(clean, zones)
    _write(demand, args.output, "m2", partition_by="pickup_borough")

    # M3 —> zone/month statistics + window functions
    zone_stats = m3.zone_month_stats(clean, zones)
    zone_stats = m3.zone_windows(zone_stats)
    _write(zone_stats, args.output, "m3")

    # O1 —> fare anomalies per pickup zones and hours
    flagged = o1.flag_fare_anomalies(clean)
    anomalies = o1.anomaly_summary(flagged, zones)
    _write(anomalies, args.output, "o1")

    print(f"Wall time: {time.perf_counter() - start:.2f}s")
    spark.stop()


def _create_spark():
    spark = (SparkSession.builder
             .appName("nyc-taxi-pipeline")
             .config("spark.sql.session.timeZone", "UTC")

             # Containers write as uid 185, keep the output deletable from the host.
             .config("spark.hadoop.fs.permissions.umask-mode", "000")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _build_arg_parser():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data", nargs="+", default=["/data/parallel-computing-p1"],
                        help="One or more input directories, Parquet files or glob patterns")
    parser.add_argument("--years", default="all",
                        help="Years to load: 'all', '2019', '2015-2019' or '2015,2017-2018' "
                             "(default: all)")
    parser.add_argument("--months", default="all",
                        help="Months to load, same syntax as --years (default: all)")
    parser.add_argument("--fraction", type=_fraction_arg, default=1.0,
                        help="Fraction of records to keep, e.g. 0.01 for a smoke test (default: 1.0)")
    parser.add_argument("--seed", type=int, default=42, help="Seed for --fraction sampling")
    parser.add_argument("--partitions", type=int, default=None,
                        help="Repartition the loaded data to this many partitions")
    parser.add_argument("--output", default="/data/parallel-computing-p1/output",
                        help="Base directory, each operation writes to <output>/<op>")
    parser.add_argument("--zones", default="/data/parallel-computing-p1/taxi_zone_lookup.csv",
                        help="Path to taxi_zone_lookup.csv")
    parser.add_argument("--stats", action="store_true",
                        help="Count how many records each rule rejects (one extra pass over the data)")
    parser.add_argument("--write-clean", action="store_true",
                        help="Also write the cleaned trips to <output>/m1 (large, off by default)")
    return parser


def _write(df, output, op, partition_by=None):
    """Write one operation's result to <output>/<op> and print how long it took."""
    path = os.path.join(output, op)
    start = time.perf_counter()
    writer = df.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.parquet(path)
    print(f"{op} written to {path} in {time.perf_counter() - start:.2f}s")


def _parse_range(spec, lo, hi, what):
    """'all' | '2019' | '2015-2019' | '2015,2017,2019-2020' -> sorted list of ints."""
    if spec is None or spec == "all":
        return list(range(lo, hi + 1))
    values = set()
    for part in spec.split(","):
        part = part.strip()
        if "-" in part:
            start, end = (int(x) for x in part.split("-", 1))
            values.update(range(start, end + 1))
        elif part:
            values.add(int(part))
    bad = [v for v in values if not lo <= v <= hi]
    if bad:
        raise argparse.ArgumentTypeError(f"{what} out of range {lo}-{hi}: {sorted(bad)}")
    return sorted(values)


def _fraction_arg(value):
    f = float(value)
    if not 0 < f <= 1:
        raise argparse.ArgumentTypeError("--fraction must be in (0, 1]")
    return f


if __name__ == "__main__":
    main()
