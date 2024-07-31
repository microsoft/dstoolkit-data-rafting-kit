# Databricks notebook source
# MAGIC %pip install data_rafting_kit-0.1.0-py3-none-any.whl
# MAGIC #from data_rafting_kit import DataRaftingKit

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

from data_rafting_kit import DataRaftingKit

# COMMAND ----------

if spark.catalog.tableExists("examples.us_labor_force_statistics") == False:
    # Azure storage access info
    blob_account_name = "azureopendatastorage"
    blob_container_name = "laborstatisticscontainer"
    blob_relative_path = "lfs/"
    blob_sas_token = r""

    # Allow SPARK to read from Blob remotely
    wasbs_path = f"wasbs://{blob_container_name}@{blob_account_name}.blob.core.windows.net/{blob_relative_path}"
    spark.conf.set(
        f"fs.azure.sas.{blob_container_name}.{blob_account_name}.blob.core.windows.net",
        blob_sas_token,
    )
    print("Remote blob path: " + wasbs_path)

    # SPARK read parquet, note that it won't load any data yet by now
    us_labor_force_statistics_df = spark.read.parquet(wasbs_path)
    us_labor_force_statistics_df.write.format("delta").saveAsTable(
        "examples.us_labor_force_statistics"
    )


# COMMAND ----------

if spark.catalog.tableExists("examples.us_national_employment_hours_earnings") == False:
    # Azure storage access info
    blob_account_name = "azureopendatastorage"
    blob_container_name = "laborstatisticscontainer"
    blob_relative_path = "ehe_national/"
    blob_sas_token = r""

    # Allow SPARK to read from Blob remotely
    wasbs_path = f"wasbs://{blob_container_name}@{blob_account_name}.blob.core.windows.net/{blob_relative_path}"
    spark.conf.set(
        f"fs.azure.sas.{blob_container_name}.{blob_account_name}.blob.core.windows.net",
        blob_sas_token,
    )
    print("Remote blob path: " + wasbs_path)

    # SPARK read parquet, note that it won't load any data yet by now
    us_national_employment_hours_earnings_df = spark.read.parquet(wasbs_path)
    us_national_employment_hours_earnings_df.write.format("delta").saveAsTable(
        "examples.us_national_employment_hours_earnings"
    )


# COMMAND ----------

if spark.catalog.tableExists("examples.us_local_area_unemployment_statistics") == False:
    # Azure storage access info
    # Azure storage access info
    blob_account_name = "azureopendatastorage"
    blob_container_name = "laborstatisticscontainer"
    blob_relative_path = "laus/"
    blob_sas_token = r""

    # Allow SPARK to read from Blob remotely
    wasbs_path = f"wasbs://{blob_container_name}@{blob_account_name}.blob.core.windows.net/{blob_relative_path}"
    spark.conf.set(
        f"fs.azure.sas.{blob_container_name}.{blob_account_name}.blob.core.windows.net",
        blob_sas_token,
    )
    print("Remote blob path: " + wasbs_path)

    # SPARK read parquet, note that it won't load any data yet by now
    us_local_area_unemployment_statistics_df = spark.read.parquet(wasbs_path)
    us_local_area_unemployment_statistics_df.write.format("delta").saveAsTable(
        "examples.us_local_area_unemployment_statistics"
    )


# COMMAND ----------

us_labor_force_statistics_df = spark.read.table("examples.us_labor_force_statistics")
us_labor_force_statistics_df.printSchema()

us_national_employment_hours_earnings_df = spark.read.table(
    "examples.us_national_employment_hours_earnings"
)
us_national_employment_hours_earnings_df.printSchema()

us_local_area_unemployment_statistics_df = spark.read.table(
    "examples.us_local_area_unemployment_statistics"
)
us_local_area_unemployment_statistics_df.printSchema()


# COMMAND ----------

# Check unique values in us_labor_force_statistics
us_labor_force_statistics_df.select("series_id", "year", "period").distinct().show(10)

# Check unique values in us_national_employment_hours_earnings
us_national_employment_hours_earnings_df.select(
    "series_id", "year", "period"
).distinct().show(10)


# COMMAND ----------

yaml_config = """
env:
  target: local

pipeline:
  inputs:
    - type: delta_table
      name: us_labor_force_statistics
      params:
        table: examples.us_labor_force_statistics
    - type: delta_table
      name: us_national_employment_hours_earnings
      params:
        table: examples.us_national_employment_hours_earnings
    - type: delta_table
      name: us_local_area_unemployment_statistics
      params:
        table: examples.us_local_area_unemployment_statistics

  transformations:
    - type: filter
      name: filter_labor_force_recent_years
      input_df: us_labor_force_statistics
      params:
        condition: "year >= 2019"

    - type: select
      name: select_labor_force_columns
      input_df: filter_labor_force_recent_years
      params:
        columns:
          - name: year
          - name: period
          - name: value
          - name: series_id

    - type: with_columns_renamed
      name: rename_labor_force_columns
      input_df: select_labor_force_columns
      params:
        columns_map:
          value: labor_force_value
          series_id: labor_force_series_id

    - type: filter
      name: filter_employment_hours_recent_years
      input_df: us_national_employment_hours_earnings
      params:
        condition: "year >= 2019"

    - type: select
      name: select_employment_hours_columns
      input_df: filter_employment_hours_recent_years
      params:
        columns:
          - name: year
          - name: period
          - name: value
          - name: series_id

    - type: with_columns_renamed
      name: rename_employment_hours_columns
      input_df: select_employment_hours_columns
      params:
        columns_map:
          value: employment_value
          series_id: employment_hours_series_id

    - type: filter
      name: filter_local_area_recent_years
      input_df: us_local_area_unemployment_statistics
      params:
        condition: "year >= 2019"

    - type: select
      name: select_local_area_unemployment_columns
      input_df: filter_local_area_recent_years
      params:
        columns:
          - name: year
          - name: period
          - name: value
          - name: series_id

    - type: with_columns_renamed
      name: rename_local_area_unemployment_columns
      input_df: select_local_area_unemployment_columns
      params:
        columns_map:
          value: local_area_value
          series_id: local_area_series_id

    - type: join
      name: join_labor_force_and_employment_hours
      input_df: rename_labor_force_columns
      params:
        other_df: rename_employment_hours_columns
        join_on: [year, period]
        how: inner

    - type: join
      name: join_with_local_area_unemployment
      input_df: join_labor_force_and_employment_hours
      params:
        other_df: rename_local_area_unemployment_columns
        join_on: [year, period]
        how: left

    - type: with_columns
      name: calculate_employment_unemployment_rates
      input_df: join_with_local_area_unemployment
      params:
        columns:
          - name: employment_rate
            expr: "employment_value / labor_force_value * 100"
          - name: unemployment_rate
            expr: "(1 - employment_value / labor_force_value) * 100"

    - type: window
      name: calculate_yearly_avg_employment_rate
      input_df: calculate_employment_unemployment_rates
      params:
        partition_by:
          - 'year'
        order_by:
          - 'period'
        window_function: 'avg'
        column: 'employment_rate'
        result_column: 'yearly_avg_employment_rate'

    - type: window
      name: calculate_rolling_avg_employment_rate
      input_df: calculate_employment_unemployment_rates
      params:
        partition_by:
          - 'labor_force_series_id'
        order_by:
          - 'year'
          - 'period'
        window_function: 'avg'
        column: 'employment_rate'
        result_column: 'rolling_avg_employment_rate'
        window_spec:
          frame_start: -3
          frame_end: 0

    - type: window
      name: calculate_cumulative_sum_employment_rate
      input_df: calculate_employment_unemployment_rates
      params:
        partition_by:
          - 'labor_force_series_id'
        order_by:
          - 'year'
          - 'period'
        window_function: 'sum'
        column: 'employment_rate'
        result_column: 'cumulative_sum_employment_rate'
        window_spec:
          frame_start: Window.unboundedPreceding
          frame_end: Window.currentRow

    - type: with_columns
      name: add_year_period
      input_df: calculate_employment_unemployment_rates
      params:
        columns:
          - name: 'year_period'
            expr: 'concat(year, "-", period)'

    - type: filter
      name: filter_high_employment_rate
      input_df: calculate_employment_unemployment_rates
      params:
        condition: 'employment_rate > 75'

    - type: with_columns
      name: calculate_employment_rate_stats
      input_df: calculate_employment_unemployment_rates
      params:
        columns:
          - name: 'max_employment_rate'
            expr: 'max(employment_rate) over (partition by year, labor_force_series_id)'
          - name: 'min_employment_rate'
            expr: 'min(employment_rate) over (partition by year, labor_force_series_id)'
          - name: 'avg_employment_rate'
            expr: 'avg(employment_rate) over (partition by year, labor_force_series_id)'

    - type: drop
      name: drop_unnecessary_columns
      input_df: calculate_employment_unemployment_rates
      params:
        columns:
          - 'footnote_codes'
          - 'absn_code'
          - 'activity_code'
          - 'ages_code'
          - 'cert_code'
          - 'class_code'
          - 'duration_code'
          - 'education_code'
          - 'entr_code'
          - 'expr_code'
          - 'hheader_code'
          - 'hour_code'
          - 'indy_code'
          - 'jdes_code'
          - 'look_code'
          - 'mari_code'
          - 'mjhs_code'
          - 'occupation_code'
          - 'orig_code'
          - 'pcts_code'
          - 'race_code'
          - 'rjnw_code'
          - 'rnlf_code'
          - 'rwns_code'
          - 'seek_code'
          - 'sexs_code'
          - 'tdat_code'
          - 'vets_code'
          - 'wkst_code'
          - 'born_code'
          - 'chld_code'
          - 'disa_code'
          - 'seasonal'

data_quality:
  - name: check_data
    params:
      checks:
        - type: 'expect_column_to_exist'
          params:
            column: 'year'
        - type: 'expect_column_values_to_be_in_set'
          params:
            column: 'period'
            values: ['M01', 'M02', 'M03', 'M04', 'M05', 'M06', 'M07', 'M08', 'M09', 'M10', 'M11', 'M12']
        - type: 'expect_column_values_to_be_unique'
          params:
            column: 'labor_force_series_id'
        - type: 'expect_column_values_to_be_between'
          params:
            column: 'year'
            min_value: '2000'
            max_value: '2023'

"""


# COMMAND ----------

pipeline = DataRaftingKit.from_yaml_str(spark, yaml_config, {})
pipeline.validate()

result = pipeline.execute()
display(result)

# COMMAND ----------
