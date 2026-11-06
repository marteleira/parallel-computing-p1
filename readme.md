# NYC Taxi pipeline with PySpark (Project Assignment 1)

`apps/pipeline.py` runs M1 (ingestion and cleaning), M2, M3, O1, O4 and O3 over the NYC TLC
Yellow Taxi trips and writes each result to `data/output/<op>` as Parquet.

## Environment

- Spark 4.2.0 standalone (`spark:python3` image, Java 21), Docker Desktop on WSL2.
- Measured on one laptop: Intel i5-8365U (4 cores / 8 threads), 16 GB RAM, Docker VM with
  12 GB (`%USERPROFILE%\.wslconfig`: `[wsl2]` / `memory=12GB`).
- Cluster: `docker/docker-compose.yml`, 1 master + N workers with 2 cores / 3G each.
  `apps/` and `data/` are mounted in every container.

## Get the data

Every measurement uses the full year 2019 (no cluster was available for 2016-2020).
Put the files in `data/`:

```bash
cd data
for m in {01..12}; do
  curl -fO "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-$m.parquet"
done
curl -fO https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

## Run

```bash
cd docker
docker compose up -d --scale spark-worker=2

docker compose exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --executor-memory 2g \
    /apps/parallel-computing-p1/pipeline.py \
    --data /data/parallel-computing-p1 --years 2019
```

- `--years` / `--months` choose the files, `--fraction 0.01` keeps 1% of the rows (smoke test),
  `--stats` prints how many records each cleaning rule rejects.
- `--executor-memory 2g` is required: with the 1g default the cache of the cleaned trips runs
  out of heap. The only non-default Spark setting in the code is
  `spark.sql.optimizer.canChangeCachedPlanOutputPartitioning=true` (`apps/pipeline.py`).
- With 1 worker the full year does not fit (one 2g executor cannot hold the cache); use 2+.
- On Git Bash for Windows, prefix the `exec` with `MSYS_NO_PATHCONV=1`.

## Reproduce the measurements

```bash
bench/benchmark.sh <commit> <label> [runs] [workers] [years] [fraction] [executor_mem]
bench/benchmark.sh ccbb59f baseline 3 2 2019 1.0 2g    # baseline
bench/benchmark.sh HEAD final 3 2 2019 1.0 2g          # final version
bench/benchmark.sh HEAD scale-w3-f0.5 1 3 2019 0.5 2g  # scalability, 3 workers, half the data
```

The script runs the code of `<commit>` (exported with `git archive`, the working tree is not
touched) with the Spark event log on, and appends:

- `report/results/benchmark.csv`: time per operation and wall time of each run;
- `report/results/stages.csv`: per-stage tasks, shuffle, spill and task-time skew, extracted
  from the event log by `bench/stage_metrics.py` (needs Python >= 3.14 for zstd logs).

Event logs stay in `data/spark-events`. To browse them in the Spark UI:

```bash
docker run -d --name spark-history -p 18080:18080 \
    -v "$PWD/data/spark-events:/events" \
    -e SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file:///events" \
    spark:python3 /opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer
# then open http://localhost:18080
```

The results section of the report is `report/sections/results.tex`.
