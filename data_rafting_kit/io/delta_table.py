# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from typing import Literal

from delta.tables import DeltaTable
from pydantic import Field, model_validator
from pyspark.sql import DataFrame

from data_rafting_kit.common.base_spec import BaseParamSpec
from data_rafting_kit.io.io_base import (
    BatchOutputModeEnum,
    InputBaseParamSpec,
    InputBaseSpec,
    IOBase,
    IOEnum,
    OutputBaseParamSpec,
    OutputBaseSpec,
)


class DeltaTableOptimizeSpec(BaseParamSpec):
    """Delta Table optimize specification."""

    z_order: list[str] | None = Field(default=None)
    vacuum: int | None = Field(default=None)
    compact: bool | None = Field(default=None)


class ColumnSourceTargetPair(BaseParamSpec):
    """Column source and target pair."""

    column: str
    source: str


class DeltaTableMergeSpec(BaseParamSpec):
    """Delta Table merge specification."""

    create_target_if_not_exists: bool | None = Field(default=True)
    condition: str
    when_matched: list[ColumnSourceTargetPair] | dict[str, str] | None = Field(
        default=None
    )
    when_not_matched: list[ColumnSourceTargetPair] | dict[str, str] | None = Field(
        default=None
    )
    when_not_matched_by_source: (
        Literal["delete"] | list[ColumnSourceTargetPair] | dict[str, str] | None
    ) = Field(default=None)


# The following classes are used to define the input and output specifications for the DeltaTable.
class DeltaTableOutputParamSpec(OutputBaseParamSpec):
    """Delta Table output parameters."""

    table: str | None = Field(default=None)
    location: str | None = Field(default=None)
    partition_by: list[str] | None = Field(default=None)
    optimize: DeltaTableOptimizeSpec | None = Field(default=None, alias="optimise")
    merge_spec: DeltaTableMergeSpec | None = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def validate_delta_table_output_param_spec_before(cls, data: dict) -> dict:
        """Validates the Delta Table output param spec."""
        if "streaming" in data and data["streaming"] is not None:
            if isinstance(data["streaming"], bool):
                data["streaming"] = {}

            if "checkpoint" not in data["streaming"]:
                table_name = (
                    data["table"]
                    if "table" in data and data["table"] is not None
                    else data["location"].split("/")[-1]
                )
                data["streaming"][
                    "checkpoint"
                ] = f"/.checkpoints/delta_table/{table_name}"

        return data

    @model_validator(mode="after")
    def validate_delta_table_output_param_spec_after(self):
        """Validates the output specification."""
        if self.mode == BatchOutputModeEnum.MERGE and self.merge_spec is None:
            raise ValueError("Merge specification is required when mode is 'merge'.")
        elif self.mode != BatchOutputModeEnum.MERGE and self.merge_spec is not None:
            raise ValueError(
                "Merge specification is not allowed when mode is not 'merge'."
            )

        if self.table is None and self.location is None:
            raise ValueError("Table or location is required.")

        return self


class DeltaTableOutputSpec(OutputBaseSpec):
    """Delta Table output specification."""

    type: Literal[IOEnum.DELTA_TABLE]
    params: DeltaTableOutputParamSpec


class DeltaTableInputParamSpec(InputBaseParamSpec):
    """Delta Table input parameters."""

    table: str | None = Field(default=None)
    location: str | None = Field(default=None)

    @model_validator(mode="after")
    def validate_delta_table_input_param_spec(self):
        """Validates the input specification."""
        if self.table is None and self.location is None:
            raise ValueError("Table or location is required.")

        return self


class DeltaTableInputSpec(InputBaseSpec):
    """Delta Table input specification."""

    type: Literal[IOEnum.DELTA_TABLE]
    params: DeltaTableInputParamSpec


class DeltaTableIO(IOBase):
    """Represents a Delta Table object for data pipelines."""

    def read(self, spec: DeltaTableInputSpec) -> DataFrame:
        """Reads from a Delta Table.

        Args:
        ----
            spec (DeltaTableInputSpec): The input parameter specification object.

        Returns:
        -------
            DataFrame: The DataFrame object.
        """
        self._logger.info("Reading from Delta Table...")

        reader = self._spark.readStream if spec.params.streaming else self._spark.read

        reader = reader.options(**spec.params.options)

        if (
            spec.params.streaming is not None
            and spec.params.streaming.watermark is not None
        ):
            reader = reader.withWatermark(
                spec.params.streaming.watermark.column,
                spec.params.streaming.watermark.duration,
            )

        if spec.params.table is not None:
            return reader.table(spec.params.table)
        else:
            return reader.format("delta").load(spec.params.location)

    def table_exists(self, spec: DeltaTableOutputSpec) -> bool:
        """Checks if a table exists.

        Args:
        ----
            spec (DeltaTableOutputSpec): The output parameter specification object.

        Returns:
        -------
            bool: True if the table exists, False otherwise.
        """
        if spec.params.table is not None:
            return self._spark.catalog.tableExists(spec.params.table)
        else:
            return DeltaTable.isDeltaTable(self._spark, spec.params.location)

    def optimize_table(self, spec: DeltaTableOutputSpec):
        """Optimizes a Delta Table.

        Args:
        ----
            spec (DeltaTableOutputSpec): The output parameter specification object.
        """
        delta_table = None
        if spec.params.table is not None:
            delta_table = DeltaTable.forName(self._spark, spec.params.table)
        else:
            delta_table = DeltaTable.forPath(self._spark, spec.params.location)

        if spec.params.optimize is not None:
            if spec.params.optimize.compact:
                delta_table.optimize().executeCompaction()

            if spec.params.optimize.z_order is not None:
                delta_table.optimize().executeZOrderBy(spec.params.optimize.z_order)

            if spec.params.optimize.vacuum is not None:
                delta_table.vacuum(spec.params.optimize.vacuum)

    def build_merge_columns(
        self, columns: list[ColumnSourceTargetPair] | dict[str, str]
    ):
        """Builds the merge columns.

        Args:
        ----
            columns (list[ColumnSourceTargetPair]): The list of columns.

        Returns:
        -------
            dict: The dictionary of columns.
        """
        if isinstance(columns, dict):
            return columns
        else:
            return {column.column: column.source for column in columns}

    def merge_into_table(self, spec: DeltaTableOutputSpec, input_df: DataFrame):
        """Merges into a Delta Table.

        Args:
        ----
            spec (DeltaTableOutputSpec): The output parameter specification object.
        input_df (DataFrame): The DataFrame object to merge.
        """
        if spec.params.table is not None:
            delta_table = DeltaTable.forName(self._spark, spec.params.table)
        else:
            delta_table = DeltaTable.forPath(self._spark, spec.params.location)

        merge_query = delta_table.alias("target").merge(
            input_df.alias("source"),
            condition=spec.params.merge_spec.condition,
        )

        if spec.params.merge_spec.when_matched is not None:
            merge_query = merge_query.whenMatchedUpdate(
                self.build_merge_columns(spec.params.merge_spec.when_matched)
            )
        else:
            merge_query = merge_query.whenMatchedUpdateAll()

        if spec.params.merge_spec.when_not_matched is not None:
            merge_query = merge_query.whenNotMatchedInsert(
                self.build_merge_columns(spec.params.merge_spec.when_not_matched)
            )
        else:
            merge_query = merge_query.whenNotMatchedInsertAll()

        if (
            spec.params.merge_spec.when_not_matched_by_source is not None
            and spec.params.merge_spec.when_not_matched_by_source == "delete"
        ):
            merge_query = merge_query.whenNotMatchedBySourceDelete()
        elif spec.params.merge_spec.when_not_matched_by_source is not None:
            merge_query = merge_query.whenNotMatchedBySourceUpdate(
                self.build_merge_columns(
                    spec.params.merge_spec.when_not_matched_by_source
                )
            )

        merge_query.execute()

    def write(self, spec: DeltaTableOutputSpec, input_df: DataFrame):
        """Writes to a Delta Table.

        Args:
        ----
            spec (DeltaTableOutputSpec): The output parameter specification object.
            input_df (DataFrame): The DataFrame object to write.
        """
        self._logger.info("Writing to Delta Table...")

        table_exists = self.table_exists(spec)
        if spec.params.mode == BatchOutputModeEnum.MERGE and table_exists:
            # Perform a merge operation
            self.merge_into_table(spec, input_df)

        elif (
            spec.params.mode == BatchOutputModeEnum.MERGE
            and not table_exists
            and spec.params.merge_spec.create_target_if_not_exists is False
        ):
            raise ValueError("Table does not exist. Cannot perform merge operation.")
        else:
            print(spec.params)
            if spec.params.mode == BatchOutputModeEnum.MERGE:
                spec.params.mode = BatchOutputModeEnum.OVERWRITE

            if spec.params.streaming is not None:
                writer = input_df.writeStream.outputMode(spec.params.mode.value)
            else:
                writer = input_df.write.mode(spec.params.mode)

            writer = writer.format("delta").options(**spec.params.options)

            if spec.params.partition_by is not None:
                writer = writer.partitionBy(**spec.params.partition_by)

            if spec.params.streaming is None:
                if spec.params.table is not None:
                    writer.saveAsTable(spec.params.table)

                    return None
                else:
                    writer = writer.option("path", spec.params.location)
            elif spec.params.table is not None:
                writer = writer.toTable(spec.params.table)
            else:
                writer = writer.option("path", spec.params.location)

            return writer
