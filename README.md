# sparkopy

Pronounced "Spar-Copy"

Downloads tables from Spark and outputs them as Parquet files

## Usage Examples

Connecting to Spark Connect:

```bash
sparkopy --spark-uri "sc://titan.jsaw.io:15002" --database export_35ff1fde_0a9c_4d12_ad09_f9fb513ff566 --table events --output /tmp/events.parquet
```

Connecting to Databricks Connect (first establish a profile via [these instructions](https://docs.databricks.com/en/dev-tools/databricks-connect/python/index.html)):

```bash
sparkopy --databricks-profile DEFAULT --database jigsaw_vrdc_aws.export_35ff1fde_0a9c_4d12_ad09_f9fb513ff566 --table events --output /tmp/events.parquet
```