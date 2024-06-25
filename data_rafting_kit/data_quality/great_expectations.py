from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal

import great_expectations as gx
import pyspark.sql.functions as f
from great_expectations.core import ExpectationSuite
from great_expectations.expectations.expectation import ExpectationConfiguration
from great_expectations.expectations.registry import (
    get_expectation_impl,
    list_registered_expectation_implementations,
)
from pydantic import BaseModel, ConfigDict, Field, create_model
from pyspark.sql import DataFrame

from data_rafting_kit.data_quality.data_quality_base import (
    DataQualityBase,
    DataQualityBaseSpec,
)


class DataQualityModeEnum(StrEnum):
    """Data quality mode enumeration."""

    FAIL = "fail"
    SEPARATE = "separate"
    FLAG = "flag"
    DROP = "drop"


EXCLUDED_GREAT_EXPECTATION_CHECKS = [
    "expect_column_kl_divergence_to_be_less_than",
    "expect_column_quantile_values_to_be_between",
    "expect_table_row_count_to_equal_other_table",
]
GREAT_EXPECTATIONS_DYNAMIC_DATA_QUALITY = [
    x
    for x in list_registered_expectation_implementations()
    if x not in EXCLUDED_GREAT_EXPECTATION_CHECKS
]

STANDARD_ARG_TYPES = {
    "mostly": (float | None, Field(default=1.0, alias="mostly")),
    "column": (str, Field(required=True, alias="column")),
    "column_A": (str, Field(required=True, alias="column_a")),
    "column_B": (str, Field(required=True, alias="column_b")),
    "type_": (str, Field(required=True, alias="type")),
    "values": (list[str | int | float], Field(required=True, alias="values")),
    "value_set": (list[str | int | float], Field(required=True, alias="values")),
    "value": (int, Field(required=True, alias="value")),
    "min_value": (
        float | datetime | str | None,
        Field(default=None, alias="min_value"),
    ),
    "max_value": (
        float | datetime | str | None,
        Field(default=None, alias="max_value"),
    ),
    "parse_strings_as_datetimes": (
        bool,
        Field(default=False, alias="parse_strings_as_datetimes"),
    ),
    "strict_min": (bool, Field(default=False, alias="strict_min")),
    "strict_max": (bool, Field(default=False, alias="strict_max")),
    "ties_okay": (bool | None, Field(default=None, alias="ties_okay")),
    "or_equal": (bool | None, Field(default=None, alias="or_equal")),
    "ignore_row_if": (
        Literal["both_values_are_missing", "either_value_is_missing", "neither"],
        Field(default="both_values_are_missing", alias="ignore_row_if"),
    ),
    "value_pairs": (list[tuple[Any, Any]], Field(required=True, alias="value_pairs")),
    "value_pairs_set": (
        list[tuple[Any, Any]],
        Field(required=True, alias="value_pairs"),
    ),
    "column_index": (int | None, Field(default=None, alias="column_index")),
    "double_sided": (
        bool | None,
        Field(required=True, default=True, alias="double_sided"),
    ),
    "threshold": (int | float, Field(required=True, alias="threshold")),
    "strictly": (bool | None, Field(default=None, alias="strictly")),
    "type_list": (list[str], Field(required=True, alias="types")),
    "json_schema": (dict, Field(required=True, alias="json_schema")),
    "like_pattern": (str, Field(required=True, alias="like_pattern")),
    "match_on": (Literal["any", "all"] | None, Field(default="any", alias="match_on")),
    "like_pattern_list": (list[str], Field(required=True, alias="like_patterns")),
    "regex": (str | str, Field(required=True, alias="regex")),
    "regex_list": (str | list[str], Field(required=True, alias="regex")),
    "strftime_format": (str, Field(required=True, alias="strftime_format")),
    "column_list": (list[str], Field(required=True, alias="columns")),
    "sum_total": (float | int, Field(required=True, alias="value")),
    "exact_match": (bool | None, Field(default=True, alias="exact_match")),
    "column_set": (list[str], Field(required=True, alias="columns")),
    "column_values": (list[str], Field(required=True, alias="columns")),
}

EXCLUDED_ARG_TYPES = ["auto", "profiler_config", "allow_cross_type_comparisons"]
TYPE_CHECKING = True


class GreatExpectationBaseSpec(DataQualityBaseSpec):
    """Base expectation parameter specification."""

    pass


dynamic_great_expectations_data_quality_models = []
for expectation_name in GREAT_EXPECTATIONS_DYNAMIC_DATA_QUALITY:
    param_fields = {}
    expectation_class = get_expectation_impl(expectation_name)
    arg_keys = set(expectation_class.args_keys + expectation_class.success_keys)

    for arg in arg_keys:
        if arg in EXCLUDED_ARG_TYPES:
            continue
        elif arg in STANDARD_ARG_TYPES:
            param_fields[arg] = STANDARD_ARG_TYPES[arg]
        else:
            raise ValueError(f"Couldn't find type for {arg} in {expectation_class}")

    param_config = ConfigDict(allow_population_by_field_name=True)
    dynamic_data_quality_param_model = create_model(
        f"{expectation_name}_params", **param_fields, __config__=param_config
    )

    fields = {
        "type": Annotated[Literal[expectation_name], Field(...)],
        "params": Annotated[dynamic_data_quality_param_model, Field(...)],
    }

    normalised_expectation_name = (
        expectation_name.replace("_", " ").title().replace(" ", "")
    )
    model_name = f"GreatExpectations{normalised_expectation_name}DataQualitySpec"

    dynamic_great_expectations_data_quality_model = create_model(
        model_name, **fields, __base__=BaseModel
    )
    dynamic_great_expectations_data_quality_models.append(
        dynamic_great_expectations_data_quality_model
    )

GREAT_EXPECTATIONS_DATA_QUALITY_SPECS = dynamic_great_expectations_data_quality_models


class GreatExpectationBaseSpec(DataQualityBaseSpec):
    """Base expectation parameter specification."""

    pass


dynamic_great_expectations_data_quality_models = []
for expectation_name in GREAT_EXPECTATIONS_DYNAMIC_DATA_QUALITY:
    param_fields = {}
    expectation_class = get_expectation_impl(expectation_name)
    arg_keys = set(expectation_class.args_keys + expectation_class.success_keys)

    for arg in arg_keys:
        if arg in EXCLUDED_ARG_TYPES:
            continue
        elif arg in STANDARD_ARG_TYPES:
            param_fields[arg] = STANDARD_ARG_TYPES[arg]
        else:
            raise ValueError(f"Couldn't find type for {arg} in {expectation_class}")

    param_config = ConfigDict(allow_population_by_field_name=True)
    dynamic_data_quality_param_model = create_model(
        f"{expectation_name}_params", **param_fields, __config__=param_config
    )

    fields = {
        "type": Annotated[Literal[expectation_name], Field(...)],
        "params": Annotated[dynamic_data_quality_param_model, Field(...)],
    }

    normalised_expectation_name = (
        expectation_name.replace("_", " ").title().replace(" ", "")
    )
    model_name = f"GreatExpectations{normalised_expectation_name}DataQualitySpec"

    dynamic_great_expectations_data_quality_model = create_model(
        model_name, **fields, __base__=BaseModel
    )
    dynamic_great_expectations_data_quality_models.append(
        dynamic_great_expectations_data_quality_model
    )

GREAT_EXPECTATIONS_DATA_QUALITY_SPECS = dynamic_great_expectations_data_quality_models


class GreatExpectationsDataQuality(DataQualityBase):
    """Represents a Great Expectations data quality expectation object."""

    def escape_quotes(self, sql_expr: Any) -> str:
        """Escapes quotes in SQL expressions.

        Args:
        ----
            sql_expr (any): The SQL expression to escape.

        Returns:
        -------
            str: The escaped SQL expression.
        """
        if isinstance(sql_expr, list):
            return f"[{', '.join([self.escape_quotes(item) for item in sql_expr])}]"
        elif isinstance(sql_expr, str):
            escaped_sql_expr = sql_expr.replace("'", "\\'")
            return f"'{escaped_sql_expr}'"
        return f"'{sql_expr}'"  # Ensure non-string values are also quoted

    def get_filter_expression(
        self, unique_column_identifiers: list, results: list
    ) -> str | None:
        """Gets the filter expression for the failing rows.

        Args:
        ----
            unique_column_identifiers (list): The unique column identifiers.
            results (list): The list of results.

        Returns:
        -------
            str | None: The filter expression. Returns none if all the rows fail.
        """
        row_filter_query = {}

        for column_identifier in unique_column_identifiers:
            row_filter_query[column_identifier] = set()

        for result in results.results:
            if result.success is False:
                unexpected_index_list = result.result.get("unexpected_index_list", [])
                print(f"Unexpected index list: {unexpected_index_list}")
                if len(unexpected_index_list) == 0:
                    # Handle case where unexpected_index_list is not provided
                    unexpected_values = result.result.get("unexpected_values", [])
                    print(f"Unexpected values: {unexpected_values}")
                    if len(unexpected_values) > 0:
                        column = result.expectation_config["kwargs"]["column"]
                        row_filter_query[column] = {
                            self.escape_quotes(value) for value in unexpected_values
                        }
                    else:
                        return None
                else:
                    for unexpected_index in unexpected_index_list:
                        for column_identifier in unique_column_identifiers:
                            row_filter_query[column_identifier].add(
                                self.escape_quotes(unexpected_index[column_identifier])
                            )

        index_filter_query_set_parts = []
        for column_identifier in unique_column_identifiers:
            if len(row_filter_query[column_identifier]) > 0:
                formatted_values = ", ".join(row_filter_query[column_identifier])
                index_filter_query_set_parts.append(
                    f"{column_identifier} IN ({formatted_values})"
                )

        filtered_query = " AND ".join(index_filter_query_set_parts)
        print(f"Generated filter query: {filtered_query}")
        return filtered_query if filtered_query else None

    def split_dataframe(
        self,
        failed_flag_column_name,
        combined_filter_expression,
        input_df: DataFrame,
        spec: GreatExpectationBaseSpec,
    ) -> tuple[DataFrame, DataFrame | None]:
        """Splits the input DataFrame based on the data quality mode.

        Args:
        ----
            failed_flag_column_name (str): The failed flag column name.
            combined_filter_expression (str): The combined filter expression.
            input_df (DataFrame): The input DataFrame.
            spec (GreatExpectationBaseSpec): The data quality expectation specification.

        Returns:
        -------
            tuple[DataFrame, DataFrame | None]: The output DataFrame and the failing rows DataFrame.
        """
        if spec.mode == DataQualityModeEnum.FLAG:
            if combined_filter_expression is None:
                input_df = input_df.withColumn(
                    failed_flag_column_name,
                    f.lit(False),
                )
            else:
                input_df = input_df.withColumn(
                    failed_flag_column_name,
                    f.when(f.expr(combined_filter_expression), f.lit(True)).otherwise(
                        f.lit(False)
                    ),
                )

            return input_df, None
        elif spec.mode in [DataQualityModeEnum.SEPARATE, DataQualityModeEnum.DROP]:
            if combined_filter_expression is None:
                empty_df = self._spark.createDataFrame([], schema=input_df.schema)

                return empty_df, input_df
            else:
                failing_rows_df = input_df.filter(f.expr(combined_filter_expression))

                input_df = input_df.subtract(failing_rows_df)

            if spec.mode == DataQualityModeEnum.DROP:
                return input_df, None

            return input_df, failing_rows_df

    def expectation(
        self, spec: GreatExpectationBaseSpec, input_df: DataFrame
    ) -> tuple[DataFrame, DataFrame] | DataFrame:
        """Executes the data quality expectation.

        Args:
        ----
            spec (GreatExpectationBaseSpec): The data quality expectation specification.
            input_df (DataFrame): The input DataFrame.

        Returns:
        -------
            DataFrame: The output DataFrame.
            DataFrame: The failing rows DataFrame.
        """
        context = gx.get_context()
        asset = context.sources.add_spark(
            "spark", spark_config=self._spark.sparkContext.getConf().getAll()
        ).add_dataframe_asset(spec.name)

        validator = context.get_validator(
            batch_request=asset.build_batch_request(dataframe=input_df)
        )

        expectation_configs = []
        for expectation in spec.checks:
            expectation_config = ExpectationConfiguration(
                expectation_type=expectation.root.type,
                kwargs=expectation.root.params.model_dump(by_alias=False),
            )
            expectation_configs.append(expectation_config)

        expectation_suite = ExpectationSuite(
            expectation_suite_name=spec.name, expectations=expectation_configs
        )

        # Check that the column identifiers exist in the input DataFrame
        for column in spec.unique_column_identifiers:
            if column not in input_df.columns:
                raise ValueError(
                    f"Column Identifier {column} not found in input DataFrame"
                )

        results = validator.validate(
            expectation_suite=expectation_suite,
            result_format={
                "result_format": "COMPLETE",
                "include_unexpected_rows": True,
                "return_unexpected_index_query": True,
                "unexpected_index_column_names": spec.unique_column_identifiers,
            },
            only_return_failures=True,
        )

        print("Validation Results:")
        print(results)

        # Add debug statement here to check unexpected index list
        for result in results.results:
            if result.success is False:
                unexpected_index_list = result.result.get("unexpected_index_list", [])
                print(
                    f"Unexpected index list for {result.expectation_config['expectation_type']}: {unexpected_index_list}"
                )

        failed_checks = [
            result["expectation_config"]["expectation_type"]
            for result in results.results
            if result.success is False
        ]

        if spec.mode == DataQualityModeEnum.FAIL and results["success"] is False:
            failed_checks_str = ", ".join(failed_checks)
            raise ValueError(f"Data quality check(s) failed: {failed_checks_str}")

        elif spec.mode in [
            DataQualityModeEnum.SEPARATE,
            DataQualityModeEnum.FLAG,
            DataQualityModeEnum.DROP,
        ]:
            failed_flag_column_name = f"failed_{spec.name}_dq"
            if results["success"] is True:
                if spec.mode == DataQualityModeEnum.FLAG:
                    failing_rows_df = None
                    input_df = input_df.withColumn(
                        failed_flag_column_name, f.lit(False)
                    )
                else:
                    failing_rows_df = self._spark.createDataFrame(
                        [], schema=input_df.schema
                    )
            else:
                combined_filter_expression = self.get_filter_expression(
                    spec.unique_column_identifiers, results
                )

                print("Combined Filter Expression:")
                print(combined_filter_expression)

                input_df, failing_rows_df = self.split_dataframe(
                    failed_flag_column_name, combined_filter_expression, input_df, spec
                )

            if failing_rows_df is not None:
                print("Failing Rows DataFrame:")
                failing_rows_df.show()
                return input_df, failing_rows_df

            return input_df
