import re
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
    "expect_column_values_to_match_strftime_format",
    "expect_column_values_to_be_json_parseable",
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


class GreatExpectationsDataQuality(DataQualityBase):
    """Represents a Great Expectations data quality expectation object."""

    def fix_unquoted_strings(self, sql_expr: str) -> str:
        """Fixes unquoted strings in SQL expressions.

        Args:
        ----
            sql_expr (str): The SQL expression to fix.

        Returns:
        -------
            str: The fixed SQL expression.
        """
        # Regex pattern to match unquoted strings inside parentheses
        pattern = r"(\b(?:NOT\s+)?(?:IN|RLIKE)\s*\()([a-zA-Z0-9_,\s]+)(\))"

        # Function to add quotes around the unquoted strings within parentheses
        def add_quotes(match: str) -> str:
            """Add quotes around the unquoted strings within parentheses.

            Args:
            ----
                match (str): The matched string.

            Returns:
            -------
                str: The fixed expression.
            """
            # Get the list of items inside the parentheses
            items = match.group(2).split(",")
            # Strip and quote each item
            quoted_items = [
                f"'{item.strip()}'"
                if not item.strip().startswith("'")
                else item.strip()
                for item in items
            ]
            # Join the quoted items back into a string
            quoted_str = ", ".join(quoted_items)
            # Return the fixed expression
            return f"{match.group(1)}{quoted_str}{match.group(3)}"

        # Fix the expressions within parentheses
        fixed_expr = re.sub(pattern, add_quotes, sql_expr)

        # Regex pattern to match unquoted strings after comparison operators
        comparison_pattern = r"(\s(=|!=|<|>|<=|>=|<=>)\s)([a-zA-Z0-9_]+)"

        # Function to add quotes around the unquoted strings after comparison operators
        def add_quotes_comparison(match: str) -> str:
            """Add quotes around unquoted strings after comparison operators.

            Args:
            ----
                match (str): The matched string.

            Returns:
            -------
                str: The fixed expression.
            """
            # Get the comparison value
            value = match.group(3)
            # Quote the value if it's not already quoted
            quoted_value = f"'{value}'" if not value.startswith("'") else value
            # Return the fixed expression
            return f"{match.group(1)}{quoted_value}"

        # Fix the expressions after comparison operators
        fixed_expr = re.sub(comparison_pattern, add_quotes_comparison, fixed_expr)

        return fixed_expr

    def get_filter_expression(self, results: list) -> str:
        """Get the combined filter expression from the results.

        Args:
        ----
            results (list): The list of results.

        Returns:
        -------
        str: The combined filter expression.
        """
        filter_expressions = []
        for result in results.results:
            if "unexpected_index_query" in result.result:
                filter_expression_pattern = r"df\.filter\(F\.expr\((.*)\)\)"
                filter_expression = re.search(
                    filter_expression_pattern,
                    result.result["unexpected_index_query"],
                ).group(1)
                print(filter_expression)
                fixed_filter_expression = self.fix_unquoted_strings(filter_expression)
                filter_expressions.append(f"({fixed_filter_expression})")
            else:
                filter_expressions.append("true")

        # Combine all filter expressions into a single expression
        combined_filter_expression = " AND ".join(filter_expressions)

        return combined_filter_expression

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
                    input_df = input_df.withColumn(
                        failed_flag_column_name, f.lit(False)
                    )
                else:
                    failing_rows_df = self._spark.createDataFrame(
                        [], schema=input_df.schema
                    )
                    return input_df, failing_rows_df
            else:
                combined_filter_expression = self.get_filter_expression(results)

                if spec.mode == DataQualityModeEnum.FLAG:
                    input_df = input_df.withColumn(
                        failed_flag_column_name,
                        f.when(
                            f.expr(combined_filter_expression), f.lit(True)
                        ).otherwise(f.lit(False)),
                    )

                    return input_df
                elif spec.mode == DataQualityModeEnum.SEPARATE:
                    failing_rows_df = input_df.filter(
                        f.expr(combined_filter_expression)
                    )
                    input_df = input_df.subtract(failing_rows_df)

                    if spec.mode == DataQualityModeEnum.SEPARATE:
                        return input_df, failing_rows_df

            return input_df
