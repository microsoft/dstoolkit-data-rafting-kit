# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# Databricks notebook source
# MAGIC %md
# MAGIC # New York Taxi Dataset Example
# MAGIC
# MAGIC An example of how to use the Data Rafting Kit with the New York Taxi Yellow Dataset

# COMMAND ----------

from data_rafting_kit import DataRaftingKit

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS examples")

# COMMAND ----------

# Ingest taken from: https://learn.microsoft.com/en-us/azure/open-datasets/dataset-taxi-yellow?tabs=pyspark
if spark.catalog.tableExists("examples.new_york_taxi_yellow") == False:
    # Azure storage access info
    blob_account_name = "azureopendatastorage"
    blob_container_name = "nyctlc"
    blob_relative_path = "yellow"
    blob_sas_token = "r"

    # Allow SPARK to read from Blob remotely
    wasbs_path = "wasbs://%s@%s.blob.core.windows.net/%s" % (
        blob_container_name,
        blob_account_name,
        blob_relative_path,
    )
    spark.conf.set(
        "fs.azure.sas.%s.%s.blob.core.windows.net"
        % (blob_container_name, blob_account_name),
        blob_sas_token,
    )
    print("Remote blob path: " + wasbs_path)

    # SPARK read parquet, note that it won't load any data yet by now
    new_york_taxi_yellow_df = spark.read.parquet(wasbs_path)
    new_york_taxi_yellow_df.write.format("delta").saveAsTable(
        "examples.new_york_taxi_yellow"
    )

# COMMAND ----------

yaml_config = """
name: ny_taxi_single_passenger_trips
env:
  target: databricks
pipeline:
  inputs:
    - type: delta_table
      name: input_delta_table
      params:
        table: examples.new_york_taxi_yellow
  transformations:
    - type: filter
      name: filter_to_single_passenger_trips
      params:
        condition: passengerCount == 1
  outputs:
    - type: delta_table
      name: output_delta_table
      params:
        table: examples.single_passenger_taxi_journeys
"""

# COMMAND ----------

pipeline = DataRaftingKit.from_yaml_str(spark, yaml_config, {})
pipeline.validate()

result = pipeline.execute()
display(result)
