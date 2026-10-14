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

    # Next modules (M2, ...) take "clean" from here

    if args.output:
        clean.write.mode("overwrite").parquet(args.output)
        print(f"Wrote cleaned data to {args.output}")
    elif not args.stats:
        print(f"Valid records: {clean.count():,}")

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
    parser.add_argument("--output", default=None,
                        help="Write the cleaned data as Parquet to this path")
    parser.add_argument("--stats", action="store_true",
                        help="Count how many records each rule rejects (one extra pass over the data)")
    return parser


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
