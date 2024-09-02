# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from datetime import datetime
from typing import Annotated, Literal

import pyspark.sql.types as t
from great_expectations.core import ExpectationSuite
from great_expectations.expectations.expectation import ExpectationConfiguration
from pydantic import Field, create_model
from pyspark.sql import DataFrame

from data_rafting_kit.common.base_spec import BaseParamSpec
from data_rafting_kit.data_quality.checks import ChecksDataQualityRootSpec
from data_rafting_kit.data_quality.data_quality_base import (
    DataQualityBase,
    DataQualityBaseSpec,
    DataQualityEnum,
)

param_fields = {
    "checks": Annotated[list[ChecksDataQualityRootSpec], Field(...)],
    "column_wise": Annotated[bool | None, Field(default=False)],
}
MetricsDataQualityParamSpec = create_model(
    "MetricsDataQualityParamSpec", **param_fields, __base__=BaseParamSpec
)

fields = {
    "input_df": Annotated[str | None, Field(default=None)],
    "params": Annotated[MetricsDataQualityParamSpec, Field(...)],
    "type": Annotated[Literal[DataQualityEnum.METRICS], Field(...)],
}
MetricsDataQualitySpec = create_model(
    "MetricsDataQualitySpec", **fields, __base__=DataQualityBaseSpec
)


class MetricsDataQuality(DataQualityBase):
    """Represents a Great Expectations data quality expectation object."""

    def filter_and_build_expectations_configurations_for_select_columns(
        self,
        spec: type[MetricsDataQualityParamSpec],
        columns: list[str],
        expectation_types: list[str],
    ):
        """Filters the expectation specs for the columns and expectation types.

        Args:
        ----
            spec (MetricsDataQualityParamSpec): The data quality expectation specification.
            columns (list[str]): The list of columns.
            expectation_types (list[str]): The list of expectation types.

        Returns:
        -------
            list[ChecksDataQualityRootSpec]: The filtered expectation specs.
        """
        expectation_configs = []
        for expectation in spec.params.checks:
            if (
                expectation.root.type in expectation_types
                and expectation.root.params.column in columns
            ):
                expectation_config = ExpectationConfiguration(
                    expectation_type=expectation.root.type,
                    kwargs=expectation.root.params.model_dump(by_alias=False),
                )
                expectation_configs.append(expectation_config)

        return expectation_configs

    def run_expectation(self, expectation_suite: ExpectationSuite) -> float:
        """Runs the expectation suite and returns the unexpected percent.

        Args:
        ----
            expectation_suite (ExpectationSuite): The expectation suite object.

        Returns:
        -------
            float: The successful percentage.
        """
        results = self._validator.validate(
            expectation_suite=expectation_suite,
            result_format={
                "result_format": "BASIC",
            },
        )

        return results["statistics"]["success_percent"]

    def run_expectation_column_wise(self, expectation_suite: ExpectationSuite) -> dict:
        """Runs the expectation suite and returns the unexpected percent.

        Args:
        ----
            expectation_suite (ExpectationSuite): The expectation suite object.

        Returns:
        -------
            dict: The successful percentage.
        """
        results = self._validator.validate(
            expectation_suite=expectation_suite,
            result_format={
                "result_format": "BASIC",
            },
        )

        column_wise_result = {}
        for result in results.results:
            column = result["expectation_config"]["kwargs"]["column"]
            column_wise_result[column] = 100 - result["result"]["unexpected_percent"]

        return column_wise_result

    def run_expectation_column_wise_with_single_expectation(
        self, expectation_suite: ExpectationSuite
    ):
        """Runs the expectation suite and returns the unexpected percent for each column.

        Args:
        ----
            expectation_suite (ExpectationSuite): The expectation suite object.

        Returns:
        -------
            dict: The unexpected percent for each column.
        """
        results = self._validator.validate(
            expectation_suite=expectation_suite,
            result_format={
                "result_format": "BASIC",
            },
        )

        unexpected_percent = {}
        for result in results.results:
            expectation_config = result["expectation_config"]["kwargs"]

            # Determine the column(s) involved in the expectation
            if "column" in expectation_config:
                column = expectation_config["column"]
            elif "column_A" in expectation_config and "column_B" in expectation_config:
                column = f"{expectation_config['column_A']}, {expectation_config['column_B']}"
            else:
                column = "unknown_column"

            unexpected_percent[column] = result["result"]["unexpected_percent"]
            # Debugging
            print(f"Result for column {column}: {result}")

        return unexpected_percent

    def build_uniqueness_expectation(self, column: str) -> ExpectationConfiguration:
        """Builds the uniqueness expectation.

        Args:
        ----
            column (str): The column name.

        Returns:
        -------
            ExpectationConfiguration: The expectation configuration.
        """
        return ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_unique",
            kwargs={"column": column},
        )

    def uniqueness(
        self, spec: type[MetricsDataQualityParamSpec], input_df: DataFrame
    ) -> dict | float:
        """Runs the uniqueness check.

        Args:
        ----
            spec (MetricsDataQualityParamSpec): The data quality expectation specification.
            input_df (DataFrame): The input DataFrame.

        Returns:
        -------
            dict | float: The uniqueness metrics.

        """
        name = "uniqueness"
        expectations = []
        for column in input_df.columns:
            expectations.append(self.build_uniqueness_expectation(column))
        suite = ExpectationSuite(expectation_suite_name=name, expectations=expectations)

        if spec.params.column_wise:
            result = self.run_expectation_column_wise_with_single_expectation(suite)
        else:
            result = self.run_expectation(suite)

        return result

    def build_completeness_expectation(self, column: str) -> ExpectationConfiguration:
        """Builds the completeness expectation.

        Args:
        ----
            column (str): The column name.

        Returns:
        -------
            ExpectationConfiguration: The expectation configuration.

        """
        return ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": column},
        )

    def completeness(
        self, spec: MetricsDataQualityParamSpec, input_df: DataFrame
    ) -> dict | float:
        """Runs the completeness check.

        Args:
        ----
            spec (MetricsDataQualityParamSpec): The data quality expectation specification.
            input_df (DataFrame): The input DataFrame.

        Returns:
        -------
            dict | float: The completeness metrics.
        """
        name = "completeness"
        expectations = []

        for column in input_df.columns:
            expectations.append(self.build_completeness_expectation(column))

        suite = ExpectationSuite(expectation_suite_name=name, expectations=expectations)

        if spec.params.column_wise:
            result = self.run_expectation_column_wise_with_single_expectation(suite)
        else:
            result = self.run_expectation(suite)

        return result

    def validity(self, spec: MetricsDataQualityParamSpec) -> dict | float:
        """Runs the validity checks.

        Args:
        ----
            spec (MetricsDataQualityParamSpec): The data quality expectation specification.
            input_df (DataFrame): The input DataFrame.

        Returns:
        -------
            dict | float: The validity metrics.
        """
        print("Running validity checks...")
        name = "validity"
        expectations = []

        for check in spec.params.checks:
            kwargs = check.root.params.model_dump(by_alias=False)

            # Check and handle different column scenarios
            if "column" in kwargs:
                print(
                    f"Creating expectation {check.root.type} for column {kwargs['column']}"
                )
            elif "column_A" in kwargs and "column_B" in kwargs:
                print(
                    f"Creating expectation {check.root.type} for columns {kwargs['column_A']} and {kwargs['column_B']}"
                )
            elif "column_list" in kwargs:
                print(
                    f"Creating expectation {check.root.type} for columns {kwargs['column_list']}"
                )
            else:
                print(
                    f"Creating expectation {check.root.type} with parameters {kwargs}"
                )

            expectation_config = ExpectationConfiguration(
                expectation_type=check.root.type,
                kwargs=kwargs,
            )
            expectations.append(expectation_config)

        suite = ExpectationSuite(expectation_suite_name=name, expectations=expectations)

        if spec.params.column_wise:
            result = self.run_expectation_column_wise_with_single_expectation(suite)
        else:
            result = self.run_expectation(suite)

        return result

    def timeliness(
        self, spec: MetricsDataQualityParamSpec, input_df: DataFrame
    ) -> tuple[DataFrame, DataFrame] | DataFrame:
        """Runs the timeliness check.

        Args:
        ----
            spec (MetricsDataQualityParamSpec): The data quality expectation specification.
            input_df (DataFrame): The input DataFrame.

        Returns:
        -------
            tuple[DataFrame, DataFrame] | DataFrame: The input DataFrame and the metric DataFrame.

        """
        # Filter metrics for the datatypes

        timeliness_columns = []
        for column in input_df.schema.fields:
            if column.dataType in [t.TimestampType(), t.DateType()]:
                timeliness_columns.append(column.name)

        # Filter metrics for the datatypes
        timeliness_expectations = (
            self.filter_and_build_expectations_configurations_for_select_columns(
                spec,
                timeliness_columns,
                ["expect_column_values_to_be_between"],
            )
        )
        expectation_suite = ExpectationSuite(
            expectation_suite_name="timeliness", expectations=timeliness_expectations
        )

        if spec.params.column_wise:
            result = {column: None for column in timeliness_columns}
            tiemliness_result = self.run_expectation_column_wise(expectation_suite)
            result.update(tiemliness_result)
        else:
            result = self.run_expectation(expectation_suite)

        return result

    def build_integrity_expectation(
        self, column: str, valid_values: list
    ) -> ExpectationConfiguration:
        """Builds the integrity expectation (domain and referential).

        Args:
        ----
            column (str): The column name.
            valid_values (list): A list of valid values (could be domain values or foreign key references).

        Returns:
        -------
            ExpectationConfiguration: The expectation configuration.

        """
        return ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={"column": column, "value_set": valid_values},
        )

    def integrity(self, spec: MetricsDataQualityParamSpec) -> dict | float:
        """Runs integrit check; domain and referential integrity.

        Args:
        ----
            spec (MetricsDataQualityParamSpec): The data quality expectation specification.
            input_df (DataFrame): The input DataFrame.

        Returns:
        -------
            dict | float: The integrity metrics.

        """
        name = "integrity"
        expectations = []

        # Loop through the checks in the spec and only check for column values in set
        for check in spec.params.checks:
            if check.root.type == "expect_column_values_to_be_in_set":
                column = check.root.params.column
                value_set = check.root.params.value_set
                expectations.append(self.build_integrity_expectation(column, value_set))

        suite = ExpectationSuite(expectation_suite_name=name, expectations=expectations)

        if spec.params.column_wise:
            result = self.run_expectation_column_wise_with_single_expectation(suite)
        else:
            result = self.run_expectation(suite)

        return result

    def metric_df_schema(self, spec: type[MetricsDataQualityParamSpec]) -> t.StructType:
        """Builds the metric DataFrame schema.

        Returns
        -------
            t.StructType: The metric DataFrame schema.

        """
        schema_structs = []

        if self._run_id is not None:
            schema_structs.append(
                t.StructField("RunId", t.StringType(), True),
            )

        if spec.params.column_wise:
            schema_structs.append(
                t.StructField("Column", t.StringType(), True),
            )

        schema_structs.extend(
            [
                t.StructField("ProcessedTimestamp", t.TimestampType(), True),
                t.StructField("Completeness", t.FloatType(), True),
                t.StructField("Uniqueness", t.FloatType(), True),
                t.StructField("Validity", t.FloatType(), True),
                t.StructField("Timeliness", t.FloatType(), True),
                t.StructField("Integrity", t.FloatType(), True),
            ]
        )

        schema = t.StructType(schema_structs)

        return schema

    def create_metric_df(
        self,
        spec: type[MetricsDataQualityParamSpec],
        metric_results: dict,
        run_time: datetime,
    ) -> DataFrame:
        """Creates a DataFrame from the metric results.

        Args:
        ----
            spec (MetricsDataQualityParamSpec): The data quality expectation specification.
            metric_results (dict): The metric results.
            run_time (datetime): The run time.

        Returns:
        -------
            DataFrame: The metric DataFrame.
        """
        columns_value_pairs = {"ProcessedTimestamp": run_time}

        if self._run_id is not None:
            columns_value_pairs["RunId"] = self._run_id
        columns_value_pairs.update(metric_results)

        metric_df = self._spark.createDataFrame(
            [columns_value_pairs], schema=self.metric_df_schema(spec)
        )

        return metric_df

    def create_metric_df_column_wise(
        self,
        spec: type[MetricsDataQualityParamSpec],
        metric_results: dict,
        run_time: datetime,
    ) -> DataFrame:
        """Creates a DataFrame from the metric results.

        Args:
        ----
            spec (MetricsDataQualityParamSpec): The data quality expectation specification.
            metric_results (dict): The metric results.
            run_time (datetime): The run time.

        Returns:
        -------
            DataFrame: The metric DataFrame.
        """
        rows_to_write = {}
        all_metrics = list(metric_results.keys())

        # Iterate over each metric and its corresponding columns
        for metric, columns in metric_results.items():
            for column, value in columns.items():
                if column not in rows_to_write:
                    row = {
                        "Column": column,
                        "ProcessedTimestamp": run_time,
                    }
                    for m in all_metrics:
                        row[m] = None
                    if self._run_id is not None:
                        row["RunId"] = self._run_id
                    rows_to_write[column] = row
                rows_to_write[column][metric] = value

        rows_to_write = list(rows_to_write.values())

        metric_df = self._spark.createDataFrame(
            rows_to_write, schema=self.metric_df_schema(spec)
        )

        return metric_df

    def calculate_overall_score(self, metric_results: dict) -> float:
        """Calculates the overall data quality score using logical rules and thresholds.

        Args:
        ----
            metric_results (dict): Dictionary containing individual metric results.

        Returns:
        -------
            float: The overall data quality score.
        """
        scores = []

        # Define thresholds for critical metrics
        critical_thresholds = {
            "Completeness": 70.0,  # Below 70% completeness is critical
            "Uniqueness": 80.0,  # Below 80% uniqueness is critical
            "Validity": 75.0,  # Below 75% validity is critical
            "Timeliness": 60.0,  # Below 60% timeliness is critical
            "Integrity": 85.0,  # Below 85% integrity is critical
        }

        # Calculate adjusted scores based on thresholds
        for metric, result in metric_results.items():
            if isinstance(result, dict):  # If column-wise results, average them
                metric_score = sum(result.values()) / len(result) if result else 0.0
            elif isinstance(result, float):  # Direct metric result
                metric_score = result
            else:
                metric_score = 0.0

            # Normalize score and apply penalty for critical failures
            if metric_score < critical_thresholds[metric]:
                penalty_factor = 0.5  # Penalize critical failures more harshly
                adjusted_score = metric_score * penalty_factor
            else:
                adjusted_score = metric_score

            scores.append(adjusted_score)

        # Calculate the overall score as the average of adjusted scores
        overall_score = sum(scores) / len(scores) if scores else 0.0

        return overall_score

    def metrics(
        self,
        spec: MetricsDataQualityParamSpec,
        input_df: DataFrame,
    ) -> tuple[DataFrame, DataFrame]:
        """Runs the data quality metrics.

        Args:
        ----
            spec (MetricsDataQualityParamSpec): The data quality expectation specification.
            input_df (DataFrame): The input DataFrame.

        Returns:
        -------
            tuple[DataFrame, DataFrame]: The input DataFrame and the metric DataFrame.
        """
        self._validator = self.get_validator(spec, input_df)
        metric_results = {}

        run_time = datetime.now()

        metric_results["Completeness"] = self.completeness(spec, input_df)
        metric_results["Uniqueness"] = self.uniqueness(spec, input_df)
        metric_results["Validity"] = self.validity(spec)
        metric_results["Timeliness"] = self.timeliness(spec, input_df)
        metric_results["Integrity"] = self.integrity(spec)

        # Calculate overall score
        overall_score = self.calculate_overall_score(metric_results)

        if spec.params.column_wise:
            metric_df = self.create_metric_df_column_wise(
                spec, metric_results, run_time
            )
        else:
            metric_df = self.create_metric_df(spec, metric_results, run_time)

        # Add overall score as a separate row or create a separate DataFrame
        overall_score_row = self._spark.createDataFrame(
            [{"OverallScore": overall_score, "ProcessedTimestamp": run_time}],
            schema=t.StructType(
                [
                    t.StructField("OverallScore", t.FloatType(), True),
                    t.StructField("ProcessedTimestamp", t.TimestampType(), True),
                ]
            ),
        )

        metric_df.show()
        overall_score_row.show()

        print("Completeness Results:", metric_results["Completeness"])
        print("Uniqueness Results:", metric_results["Uniqueness"])
        print("Validity Results:", metric_results["Validity"])
        print("Timeliness Results:", metric_results["Timeliness"])
        print("Integrity Results:", metric_results["Integrity"])
        print("Overall Score:", overall_score)

        return input_df, metric_df, overall_score_row
