import click
import pandas
from pyspark.sql import SparkSession
from databricks.connect import DatabricksSession

def dump_table(spark, database_name, table_name, output):
  df = spark.read.table(f"{database_name}.{table_name}")
  df_p = df.toPandas()
  # The metrics attribute contains a Plan object that can't be
  # serialized to JSON, which happens when pyspark attempts
  # to save metadata inside the Parquet file
  del df_p.attrs['metrics']
  df_p.to_parquet(output)

def get_spark(spark_uri, databricks_profile):
  if databricks_profile is not None:
    return get_databricks_session(databricks_profile)
  else:
    return get_spark_session(spark_uri)

def get_databricks_session(databricks_profile):
  return(DatabricksSession.builder \
    .profile(databricks_profile)
    .getOrCreate())

def get_spark_session(spark_uri):
  return(SparkSession.builder \
    .remote(spark_uri) \
    .getOrCreate())

@click.command()
@click.option('--spark-uri', help='URL for Spark Connect, e.g. sc://hostname:15002')
@click.option('--databricks-profile', help='name of authenticated databricks profile, e.g. DEFAULT')
@click.option('--database', required=True, help='database to either dump completely, or search for the specified table')
@click.option('--table', required=True, help='table to dump')
@click.option('--output', required=True, help='file path for Parquet file')
def cli(spark_uri, databricks_profile, database, table, output):
  dump_table(get_spark(spark_uri, databricks_profile), database, table, output)