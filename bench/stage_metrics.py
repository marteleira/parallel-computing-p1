"""
stage_metrics.py
-----------
Extracts per-stage metrics (tasks, duration, shuffle, spill, skew) from a Spark event log
-------------------------

usage: python -I bench/stage_metrics.py <label> <app_id>
Appends one row per stage to report/results/stages.csv and prints a per-operation summary.
"""

import csv
import json
import os
import re
import statistics
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EVENTS_DIR = os.path.join(REPO, "data", "spark-events")
RESULTS = os.path.join(REPO, "report", "results", "stages.csv")
MB = 1024 * 1024

# accumulator name in the event log -> (column in stages.csv, divisor)
# input bytes are not counted by the local filesystem in these containers, so records are used
METRICS = {
    "internal.metrics.input.recordsRead": ("input_mrecords", 1e6),
    "internal.metrics.shuffle.read.localBytesRead": ("shuffle_read_mb", MB),
    "internal.metrics.shuffle.read.remoteBytesRead": ("shuffle_read_mb", MB),
    "internal.metrics.shuffle.write.bytesWritten": ("shuffle_write_mb", MB),
    "internal.metrics.memoryBytesSpilled": ("spill_memory_mb", MB),
    "internal.metrics.diskBytesSpilled": ("spill_disk_mb", MB),
}
COLUMNS = ["label", "app_id", "op", "stage_id", "stage_name", "tasks", "duration_s",
           "input_mrecords", "shuffle_read_mb", "shuffle_write_mb", "spill_memory_mb",
           "spill_disk_mb", "task_median_s", "task_max_s"]


def main():
    label, app_id = sys.argv[1], sys.argv[2]
    stages = _read_stages(_events(app_id))

    new_file = not os.path.exists(RESULTS)
    with open(RESULTS, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        if new_file:
            writer.writeheader()
        for row in stages:
            writer.writerow({"label": label, "app_id": app_id, **row})

    print(f"{'op':<6}{'stages':>8}{'tasks':>8}{'stage s':>10}{'input Mrec':>11}"
          f"{'shuffle R MB':>14}{'shuffle W MB':>14}{'spill MB':>10}{'max/median':>12}")
    for op in sorted({row["op"] for row in stages}, key=_op_order):
        rows = [row for row in stages if row["op"] == op]
        skew = max((row["task_max_s"] / row["task_median_s"] for row in rows
                    if row["task_median_s"] > 0), default=0)
        print(f"{op:<6}{len(rows):>8}{sum(r['tasks'] for r in rows):>8}"
              f"{sum(r['duration_s'] for r in rows):>10.1f}"
              f"{sum(r['input_mrecords'] for r in rows):>11,.1f}"
              f"{sum(r['shuffle_read_mb'] for r in rows):>14,.0f}"
              f"{sum(r['shuffle_write_mb'] for r in rows):>14,.0f}"
              f"{sum(r['spill_disk_mb'] for r in rows):>10,.0f}{skew:>12.1f}")


def _events(app_id):
    """Yields the JSON events of an application, the log may be split in several files."""
    folder = os.path.join(EVENTS_DIR, f"eventlog_v2_{app_id}")
    for name in sorted(os.listdir(folder), key=lambda n: int(n.split("_")[1])
                       if n.startswith("events_") else -1):
        if not name.startswith("events_"):
            continue
        with _open(os.path.join(folder, name)) as f:
            for line in f:
                yield json.loads(line)


def _open(path):
    if path.endswith(".zstd"):
        from compression import zstd  # python >= 3.14
        return zstd.open(path, "rt")
    return open(path, encoding="utf-8")


def _read_stages(events):
    """
    One dict per completed stage. Stages are mapped to an operation through their SQL execution,
    whose plan contains the output path of the write (<output>/<op>); skipped stages (reused
    shuffles or cache) never complete, so they are not counted.
    """
    execution_op, stage_execution, task_times, stages = {}, {}, {}, []
    for event in events:
        kind = event["Event"]
        if kind.endswith("SQLExecutionStart"):
            match = re.search(r"output/(\w+)", event.get("physicalPlanDescription", ""))
            execution_op[event["executionId"]] = match.group(1) if match else "other"
        elif kind == "SparkListenerJobStart":
            execution = event.get("Properties", {}).get("spark.sql.execution.id")
            for stage_id in event["Stage IDs"]:
                stage_execution[stage_id] = int(execution) if execution else None
        elif kind == "SparkListenerTaskEnd":
            info = event["Task Info"]
            task_times.setdefault(event["Stage ID"], []).append(
                (info["Finish Time"] - info["Launch Time"]) / 1000)
        elif kind == "SparkListenerStageCompleted":
            stages.append(_stage_row(event["Stage Info"], stage_execution, execution_op,
                                     task_times))
    return stages


def _stage_row(info, stage_execution, execution_op, task_times):
    stage_id = info["Stage ID"]
    times = task_times.get(stage_id, [0])
    row = {"op": execution_op.get(stage_execution.get(stage_id), "other"),
           "stage_id": stage_id, "stage_name": info["Stage Name"],
           "tasks": info["Number of Tasks"],
           "duration_s": round((info["Completion Time"] - info["Submission Time"]) / 1000, 2),
           "task_median_s": round(statistics.median(times), 2),
           "task_max_s": round(max(times), 2)}
    for column, _ in METRICS.values():
        row[column] = 0.0
    for acc in info.get("Accumulables", []):
        if acc.get("Name") in METRICS:
            column, divisor = METRICS[acc["Name"]]
            row[column] = round(row[column] + int(acc["Value"]) / divisor, 1)
    return row


def _op_order(op):
    order = ["m1", "m2", "m3", "o1", "o4", "o3"]
    return order.index(op) if op in order else len(order)


if __name__ == "__main__":
    main()
