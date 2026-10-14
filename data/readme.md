# How to use
In this folder download the dataset, can use this script:
```bash
#!/bin/bash
for y in {2009..2026}; do
  for m in {01..12}; do
    curl -fO "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_$y-$m.parquet"
  done
done
```
