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

    def filter_expectations_specs_for_columns(
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
                and expectation.root.column in columns
            ):
                expectation_configs.append(expectation)

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

            print(result)

        return column_wise_result

    def run_expectation_column_wise_with_single_expectation(
        self, expectation_suite: ExpectationSuite
    ):
        """Runs the expectation suite and returns the unexpected percent.

        Args:
        ----
            expectation_suite (ExpectationSuite): The expectation suite object.

        Returns:
        -------
            float: The unexpected percent.
        """
        results = self._validator.validate(
            expectation_suite=expectation_suite,
            result_format={
                "result_format": "BASIC",
            },
        )

        return results.results["unexpected_percent"]

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
        timeliness_expectations_spec = self.filter_expectations_specs_for_columns(
            spec,
            timeliness_columns,
            ["expect_column_values_to_be_between"],
        )
        timeliness_expectations = self.build_expectation_configuration(
            timeliness_expectations_spec, input_df
        )

        if spec.params.column_wise:
            result = self.run_expectation_column_wise(timeliness_expectations)
        else:
            result = self.run_expectation(timeliness_expectations)

        return result

    def create_metric_df(self, metric_results: dict, run_time: datetime) -> DataFrame:
        """Creates a DataFrame from the metric results.

        Args:
        ----
            metric_results (dict): The metric results.
            run_time (datetime): The run time.

        Returns:
        -------
            DataFrame: The metric DataFrame.
        """
        columns_value_pairs = {"ProcessedTimestamp": run_time}
        columns_value_pairs.update(metric_results)
        metric_df = self._spark.createDataFrame([columns_value_pairs])

        return metric_df

    def create_metric_df_column_wise(
        self, metric_results: dict, run_time: datetime
    ) -> DataFrame:
        """Creates a DataFrame from the metric results.

        Args:
        ----
            metric_results (dict): The metric results.
            run_time (datetime): The run time.

        Returns:
        -------
            DataFrame: The metric DataFrame.
        """
        rows_to_write = {}
        for metric in metric_results:
            for column in metric_results[metric]:
                if column not in rows_to_write:
                    rows_to_write[column] = {
                        "Column": column,
                        "ProcessedTimestamp": run_time,
                    }

                rows_to_write[column] = metric_results[metric][column]

        metric_df = self._spark.createDataFrame([rows_to_write.values()])

        return metric_df

    def metrics(
        self, spec: type[MetricsDataQualityParamSpec], input_df: DataFrame
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

        if spec.params.column_wise:
            metric_df = self.create_metric_df_column_wise(metric_results, run_time)
        else:
            metric_df = self.create_metric_df(metric_results, run_time)

        # Temp print
        metric_df.show()

        return input_df, metric_df
