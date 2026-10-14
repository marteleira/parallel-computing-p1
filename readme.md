### Run from the host
```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \\
        --master spark://spark-master:7077 \\
        /apps/parallel-computing-p1/pipeline.py \\
        --data /data/parallel-computing-p1 --years 2019 --fraction 0.01 --stats
```

Scale is controlled by --years / --months (which files are read) and
--fraction (which share of the rows is kept, for smoke tests).

### Get the dataset
Use this script:
```bash
#!/bin/bash
cd dataset/
for y in {2009..2026}; do
  for m in {01..12}; do
    curl -fO "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_$y-$m.parquet"
  done
done
```
