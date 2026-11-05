#!/bin/bash
# benchmark.sh
# -----------
# Runs the pipeline of one git commit on the Docker cluster and appends its timings to a CSV
# -------------------------
#
# usage: bench/benchmark.sh <commit> <label> [runs] [workers] [years] [fraction]
#   e.g. bench/benchmark.sh ccbb59f baseline 2 2 2019 1.0
#
# The code of <commit> is exported to data/bench-code/<label>, so the working tree is never
# checked out. Spark event logs go to data/spark-events (open them with the History Server).

set -euo pipefail

LABEL=$2
RUNS=${3:-1}
WORKERS=${4:-2}
YEARS=${5:-2019}
FRACTION=${6:-1.0}

REPO=$(cd "$(dirname "$0")/.." && pwd)
COMMIT=$(git -C "$REPO" rev-parse --short "$1")
COMPOSE_DIR="$REPO/../docker-spark-env"
RESULTS="$REPO/report/results/benchmark.csv"
CONTAINER_DATA=/data/parallel-computing-p1

# export the code of that commit next to the data, which is mounted in every container
CODE_DIR="$REPO/data/bench-code/$LABEL"
rm -rf "$CODE_DIR" && mkdir -p "$CODE_DIR" "$REPO/data/spark-events" "$(dirname "$RESULTS")"
git -C "$REPO" archive "$COMMIT" apps | tar -x -C "$CODE_DIR" --strip-components=1

if [ ! -f "$RESULTS" ]; then
    echo "label,commit,run,workers,years,fraction,m2_s,m3_s,o1_s,o4_s,o3_s,wall_s,app_id,date" \
        > "$RESULTS"
fi

cd "$COMPOSE_DIR"
docker compose up -d --scale spark-worker="$WORKERS" > /dev/null 2>&1
sleep 10   # let the workers register with the master

for run in $(seq 1 "$RUNS"); do
    echo "== $LABEL ($COMMIT) run $run/$RUNS, $WORKERS workers, years=$YEARS fraction=$FRACTION"
    log="$REPO/data/bench-code/$LABEL-run$run.log"

    # git bash on windows would otherwise rewrite the /data/... container paths
    MSYS_NO_PATHCONV=1 docker compose exec -T spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.eventLog.enabled=true \
        --conf spark.eventLog.dir=file://$CONTAINER_DATA/spark-events \
        --conf spark.app.name="$LABEL-run$run" \
        "$CONTAINER_DATA/bench-code/$LABEL/pipeline.py" \
        --data "$CONTAINER_DATA" --years "$YEARS" --fraction "$FRACTION" > "$log" 2>&1

    # "m2 written to ... in 15.34s" and "Wall time: 61.51s" are printed by pipeline._write/main
    op_time() { grep -oP "^$1 written to .* in \K[0-9.]+" "$log" || echo ""; }
    wall=$(grep -oP "^Wall time: \K[0-9.]+" "$log" || echo "")
    app_id=$(grep -oP "app ID \Kapp-[0-9-]+" "$log" | head -1 || echo "")

    if [ -z "$wall" ]; then
        echo "   FAILED, see $log"
        continue
    fi
    echo "$LABEL,$COMMIT,$run,$WORKERS,$YEARS,$FRACTION,$(op_time m2),$(op_time m3),$(op_time o1)," \
         "$(op_time o4),$(op_time o3),$wall,$app_id,$(date -Iseconds)" | tr -d ' ' >> "$RESULTS"
    echo "   wall time ${wall}s  (m2 $(op_time m2)s, m3 $(op_time m3)s, o1 $(op_time o1)s," \
         "o4 $(op_time o4)s, o3 $(op_time o3)s)"
    # per-stage metrics (tasks, shuffle, spill, skew) from the event log -> stages.csv
    python3 -I "$REPO/bench/stage_metrics.py" "$LABEL" "$app_id"
done
