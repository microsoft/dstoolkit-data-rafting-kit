# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import logging
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

threshold_fields = {
    "completeness": Annotated[float | None, Field(default=70.0, le=100, ge=0)],
    "uniqueness": Annotated[float | None, Field(default=80.0, le=100, ge=0)],
    "validity": Annotated[float | None, Field(default=75.0, le=100, ge=0)],
    "timeliness": Annotated[float | None, Field(default=60.0, le=100, ge=0)],
    "integrity": Annotated[float | None, Field(default=85.0, le=100, ge=0)],
    "penalty_factor": Annotated[float | None, Field(default=0.5, le=1, ge=0)],
}
MetricsDataQualityThresholdsSpec = create_model(
    "MetricsDataQualityThresholdsSpec", **threshold_fields, __base__=BaseParamSpec
)


param_fields = {
    "checks": Annotated[list[ChecksDataQualityRootSpec], Field(...)],
    "column_wise": Annotated[bool | None, Field(default=False)],
    "thresholds": Annotated[
        MetricsDataQualityThresholdsSpec | None,
        Field(default_factory=MetricsDataQualityThresholdsSpec),
    ],
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
        expectation_types: list[str] | None = None,
        columns: list[str] | None = None,
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
            if hasattr(expectation.root.params, "column"):
                column_name = expectation.root.params.column
            elif hasattr(expectation.root.params, "column_A"):
                column_name = expectation.root.params.column_A
            else:
                logging.warning(
                    "Skipping %s expectation for column-wise validity. This expectation type is not supported for column-wise validity.",
                    expectation.root.type,
                )
                continue

            if (
                expectation_types is None or expectation.root.type in expectation_types
            ) and (columns is None or column_name in columns):
                expectation_config = ExpectationConfiguration(
                    type=expectation.root.type,
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

        if results["success"] is False:
            for result in results.results:
                logging.debug(result)
                if (
                    "exception_info" in result
                    and result["exception_info"]["raised_exception"] is True
                ):
                    logging.error("Data quality checks failed due to exception.")
                    logging.error(results)
                    raise ValueError("Data quality checks failed due to exception.")

        return results["statistics"]["success_percent"]

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

        if results["success"] is False:
            for result in results.results:
                logging.debug(result)
                if (
                    "exception_info" in result
                    and result["exception_info"]["raised_exception"] is True
                ):
                    logging.error("Data quality checks failed due to exception.")
                    logging.error(results)
                    raise ValueError("Data quality checks failed due to exception.")

        unexpected_percent = {}
        for result in results.results:
            expectation_config = result["expectation_config"]["kwargs"]

            # Determine the column(s) involved in the expectation
            if "column" in expectation_config:
                column = expectation_config["column"]
            elif "column_A" in expectation_config and "column_B" in expectation_config:
                column = expectation_config["column_A"]
            else:
                raise ValueError("Column(s) not found in expectation configuration.")

            unexpected_percent[column] = 100 - result["result"]["unexpected_percent"]

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
            type="expect_column_values_to_be_unique",
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
        logging.info("Running uniqueness checks.")
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
            type="expect_column_values_to_not_be_null",
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
        logging.info("Running completeness checks.")
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

    def validity(
        self, spec: MetricsDataQualityParamSpec, input_df: DataFrame
    ) -> dict | float:
        """Runs the validity checks.

        Args:
        ----
            spec (MetricsDataQualityParamSpec): The data quality expectation specification.
            input_df (DataFrame): The input DataFrame.

        Returns:
        -------
            dict | float: The validity metrics.
        """
        logging.info("Running validity checks.")

        if spec.params.column_wise:
            expectation_columns = []
            for expectation in spec.params.checks:
                if hasattr(expectation.root.params, "column"):
                    expectation_columns.append(expectation.root.params.column)
                elif hasattr(expectation.root.params, "column_A"):
                    expectation_columns.append(expectation.root.params.column_A)
                else:
                    logging.warning(
                        "Skipping %s expectation for column-wise validity. This expectation type is not supported for column-wise validity.",
                        expectation.root.type,
                    )

            def has_duplicates(lst):
                seen = set()
                for item in lst:
                    if item in seen:
                        return True  # Duplicate found
                    seen.add(item)
                return False

            if has_duplicates(expectation_columns):
                result = {}
                for column in input_df.columns:
                    filtered_expectations = self.filter_and_build_expectations_configurations_for_select_columns(
                        spec, columns=[column]
                    )

                    if len(filtered_expectations) == 0:
                        continue

                    suite = ExpectationSuite(
                        expectation_suite_name=f"validity-{column}",
                        expectations=filtered_expectations,
                    )

                    result_for_column = self.run_expectation(suite)

                    result[column] = result_for_column
            else:
                suite = self.build_expectation_configuration(
                    spec, validate_unique_column_identifiers=False
                )

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
        logging.info("Running timeliness checks.")
        # Filter metrics for the datatypes

        timeliness_columns = []
        for column in input_df.schema.fields:
            if column.dataType in [t.TimestampType(), t.DateType()]:
                timeliness_columns.append(column.name)

        # Filter metrics for the datatypes
        expectations = (
            self.filter_and_build_expectations_configurations_for_select_columns(
                spec,
                expectation_types=["expect_column_values_to_be_between"],
                columns=timeliness_columns,
            )
        )
        if len(expectations) == 0 and spec.params.column_wise:
            return {}
        elif len(expectations) == 0:
            return None

        expectation_suite = ExpectationSuite(
            expectation_suite_name="timeliness", expectations=expectations
        )

        if spec.params.column_wise:
            result = self.run_expectation_column_wise_with_single_expectation(
                expectation_suite
            )
        else:
            result = self.run_expectation(expectation_suite)

        return result

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
        logging.info("Running integrity checks.")
        name = "integrity"

        expectations = (
            self.filter_and_build_expectations_configurations_for_select_columns(
                spec, expectation_types=["expect_column_values_to_be_in_set"]
            )
        )

        if len(expectations) == 0 and spec.params.column_wise:
            return {}
        elif len(expectations) == 0:
            return None

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
                t.StructField("Overall", t.FloatType(), True),
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

                # If 'value' a dictionary.
                if isinstance(value, dict):
                    for sub_value in value.items():
                        rows_to_write[column][metric] = sub_value
                else:
                    rows_to_write[column][metric] = value

        rows_to_write = list(rows_to_write.values())

        metric_df = self._spark.createDataFrame(
            rows_to_write, schema=self.metric_df_schema(spec)
        )

        return metric_df

    def normalise_score(self, thresholds: dict, score: float, metric: str) -> float:
        """Normalizes the score based on the metric.

        Args:
        ----
            thresholds (dict): The thresholds.
            score (float): The score.
            metric (str): The metric.

        Returns:
        -------
            float: The normalized score.
        """
        if score < thresholds[metric.lower()]:
            adjusted_score = score * thresholds["penalty_factor"]
        else:
            adjusted_score = score

        return adjusted_score

    def calculate_overall_score(
        self, spec: type[MetricsDataQualityParamSpec], metric_results: dict
    ) -> float:
        """Calculates the overall data quality score using logical rules and thresholds.

        Args:
        ----
            spec (MetricsDataQualityParamSpec): The data quality expectation specification.
            metric_results (dict): Dictionary containing individual metric results.

        Returns:
        -------
            float: The overall data quality score.
        """
        # Pull thresholds for critical metrics
        thresholds = spec.params.thresholds.model_dump(by_alias=False)

        # Calculate adjusted scores based on thresholds
        if spec.params.column_wise:
            column_wise_scores = {}
            for metric, columns in metric_results.items():
                for column, metric_score in columns.items():
                    if column not in column_wise_scores:
                        column_wise_scores[column] = []

                    if isinstance(metric_score, float):  # Direct metric result
                        # Normalize score and apply penalty for critical failures
                        adjusted_score = self.normalise_score(
                            thresholds, metric_score, metric
                        )

                        column_wise_scores[column].append(adjusted_score)

            # Calculate the overall score as the average of adjusted scores
            overall_score = {
                column: sum(scores) / len(scores) if len(scores) > 0 else None
                for column, scores in column_wise_scores.items()
            }
        else:
            scores = []

            for metric, result in metric_results.items():
                if isinstance(result, float):  # Direct metric result
                    metric_score = result

                    # Normalize score and apply penalty for critical failures
                    adjusted_score = self.normalise_score(
                        thresholds, metric_score, metric
                    )

                    scores.append(adjusted_score)

            # Calculate the overall score as the average of adjusted scores
            overall_score = sum(scores) / len(scores) if len(scores) > 0 else None

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
        metric_results["Validity"] = self.validity(spec, input_df)
        metric_results["Timeliness"] = self.timeliness(spec, input_df)
        metric_results["Integrity"] = self.integrity(spec)

        # Calculate overall score
        metric_results["Overall"] = self.calculate_overall_score(spec, metric_results)

        if spec.params.column_wise:
            metric_df = self.create_metric_df_column_wise(
                spec, metric_results, run_time
            )
        else:
            metric_df = self.create_metric_df(spec, metric_results, run_time)

        metric_df.show()

        logging.info("Data quality metrics completed.")

        logging.info("Completeness Results: %s", metric_results["Completeness"])
        logging.info("Uniqueness Results: %s", metric_results["Uniqueness"])
        logging.info("Validity Results: %s", metric_results["Validity"])
        logging.info("Timeliness Results: %s", metric_results["Timeliness"])
        logging.info("Integrity Results: %s", metric_results["Integrity"])
        logging.info("Overall Results: %s", metric_results["Overall"])

        return input_df, metric_df
