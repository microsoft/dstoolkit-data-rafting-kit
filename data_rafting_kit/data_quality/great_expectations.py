# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from datetime import datetime
from typing import Annotated, Any, Literal

import great_expectations as gx
from great_expectations.core import ExpectationSuite
from great_expectations.expectations.expectation import ExpectationConfiguration
from great_expectations.expectations.registry import (
    get_expectation_impl,
    list_registered_expectation_implementations,
)
from pydantic import BaseModel, ConfigDict, Field, create_model
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from data_rafting_kit.data_quality.data_quality_base import (
    DataQualityBase,
    DataQualityBaseSpec,
)

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
    "column_a": (str, Field(required=True, alias="column_a")),
    "column_b": (str, Field(required=True, alias="column_b")),
    "type": (str, Field(required=True, alias="type")),
    "values": (list[str | int | float], Field(required=True, alias="values")),
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
    "column_index": (int | None, Field(default=None, alias="column_index")),
    "double_sided": (bool, Field(required=True, alias="double_sided")),
    "threshold": (int | float, Field(required=True, alias="threshold")),
    "strictly": (bool | None, Field(default=None, alias="strictly")),
    "types": (list[str], Field(required=True, alias="types")),
    "json_schema": (str, Field(required=True, alias="json_schema")),
    "like_pattern": (str, Field(required=True, alias="like_pattern")),
    "match_on": (Literal["any", "all"] | None, Field(default="any", alias="match_on")),
    "like_patterns": (list[str], Field(required=True, alias="like_patterns")),
    "regex": (str | list[str], Field(required=True, alias="regex")),
    "strftime_format": (str, Field(required=True, alias="strftime_format")),
    "columns": (list[str], Field(required=True, alias="columns")),
    "sum_total": (float | int, Field(required=True, alias="sum_total")),
    "exact_match": (bool | None, Field(default=True, alias="exact_match")),
    "column_set": (list[str], Field(required=True, alias="column_set")),
}

EXCLUDED_ARG_TYPES = ["auto", "profiler_config", "allow_cross_type_comparisons"]
TYPE_CHECKING = True


class GreatExpectationBaseSpec(DataQualityBaseSpec):
    """Base expectation parameter specification."""

    pass


GREAT_EXPECTATIONS_DYNAMIC_DATA_QUALITY_PARAMATER_REPLACEMENT_MAP = {
    "expect_table_columns_to_match_ordered_list": {"column_list": "columns"},
    "expect_table_columns_to_match_set": {"column_set": "columns"},
    "expect_compound_columns_to_be_unique": {"column_list": "columns"},
    "expect_multicolumn_sum_to_equal": {"column_list": "columns"},
    "expect_select_column_values_to_be_unique_within_record": {
        "column_list": "columns"
    },
    "expect_column_values_to_not_match_regex_list": {"regex_list": "regex"},
    "expect_column_values_to_match_regex_list": {"regex_list": "regex"},
    "expect_column_values_to_not_be_in_set": {"value_set": "values"},
    "expect_column_values_to_be_in_set": {"value_set": "values"},
    "expect_column_distinct_values_to_be_in_set": {"value_set": "values"},
    "expect_column_distinct_values_to_contain_set": {"value_set": "values"},
    "expect_column_distinct_values_to_equal_set": {"value_set": "values"},
    "expect_column_most_common_value_to_be_in_set": {"value_set": "values"},
    "expect_column_values_to_not_match_like_pattern_list": {
        "like_pattern_list": "like_patterns"
    },
    "expect_column_values_to_match_like_pattern_list": {
        "like_pattern_list": "like_patterns"
    },
    "expect_column_values_to_be_in_type_list": {"type_list": "types"},
    "expect_column_pair_values_to_be_equal": {
        "column_A": "column_a",
        "column_B": "column_b",
    },
    "expect_column_pair_values_a_to_be_greater_than_b": {
        "column_A": "column_a",
        "column_B": "column_b",
    },
    "expect_column_pair_values_to_be_in_set": {
        "column_A": "column_a",
        "column_B": "column_b",
        "value_pairs_set": "value_pairs",
    },
    "expect_column_values_to_be_of_type": {"type_": "type"},
}

dynamic_great_expectations_data_quality_models = []
for expectation_name in GREAT_EXPECTATIONS_DYNAMIC_DATA_QUALITY:
    param_fields = {}
    expectation_class = get_expectation_impl(expectation_name)
    arg_keys = set(expectation_class.args_keys + expectation_class.success_keys)

    for arg in arg_keys:
        if arg in EXCLUDED_ARG_TYPES:
            continue
        else:
            try:
                cleaned_arg = (
                    GREAT_EXPECTATIONS_DYNAMIC_DATA_QUALITY_PARAMATER_REPLACEMENT_MAP[
                        expectation_name
                    ][arg]
                )
            except KeyError:
                cleaned_arg = arg

            if cleaned_arg in STANDARD_ARG_TYPES:
                param_fields[arg] = STANDARD_ARG_TYPES[cleaned_arg]
            else:
                raise ValueError(
                    f"Couldn't find type for {cleaned_arg} in {expectation_class}"
                )

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

    def expectation(self, spec: GreatExpectationBaseSpec, input_df: DataFrame):
        """Executes the data quality expectation."""
        context = gx.get_context()
        asset = context.sources.add_spark("spark").add_dataframe_asset(spec.name)

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

        results = validator.validate(
            expectation_suite=expectation_suite,
            result_format={
                "result_format": "COMPLETE",
                "include_unexpected_rows": True,
                "return_unexpected_index_query": True,
                "unexpected_index_column_names": ["show_id"],
            },
        )

        # Collect all show_ids that are unexpected for each failed check
        all_unexpected_show_ids = []
        failed_checks = []

        for result in results["results"]:
            if not result["success"]:
                expectation_type = result["expectation_config"]["expectation_type"]
                unexpected_data = result["result"].get("unexpected_index_list", [])

                show_ids = [
                    item["show_id"]
                    for item in unexpected_data
                    if isinstance(item, dict) and "show_id" in item
                ]

                all_unexpected_show_ids.append(set(show_ids))
                failed_checks.append(expectation_type)

        if all_unexpected_show_ids:
            # Perform intersection of all sets to get common unexpected show_ids
            final_unexpected_show_ids = set.intersection(*all_unexpected_show_ids)

            # Convert the set of final unexpected show_ids to a list
            final_unexpected_show_ids_list = list(final_unexpected_show_ids)

            # Filter the input DataFrame to get all unexpected rows
            unexpected_rows = input_df.filter(
                col("show_id").isin(final_unexpected_show_ids_list)
            )
            cleaned_rows = input_df.subtract(unexpected_rows)

            unexpected_rows.show(truncate=True)
            cleaned_rows.show(truncate=True)

            # Save unexpected rows to a separate file
            unexpected_rows.write.format("delta").mode("overwrite").save(
                "./data/unexpected_rows"
            )

            # Save cleaned rows to a separate file
            cleaned_rows.write.format("delta").mode("overwrite").save(
                "./data/cleaned_rows"
            )

            failed_checks_str = ", ".join(failed_checks)
            raise ValueError(f"Data quality check(s) failed: {failed_checks_str}")

        print(results)
        return input_df
