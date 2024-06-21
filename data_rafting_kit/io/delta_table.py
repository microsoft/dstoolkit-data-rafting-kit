from enum import StrEnum
from typing import Literal

from delta.tables import DeltaTable
from pydantic import BaseModel, Field, model_validator
from pyspark.sql import DataFrame

from data_rafting_kit.io.io_base import (
    InputBaseParamSpec,
    InputBaseSpec,
    IOBase,
    IOEnum,
    OutputBaseParamSpec,
    OutputBaseSpec,
)


class DeltaTableModeEnum(StrEnum):
    """Enumeration class for Delta Table modes."""

    APPEND = "append"
    OVERWRITE = "overwrite"
    ERROR = "error"
    IGNORE = "ignore"
    MERGE = "merge"


class DeltaTableOptimizeSpec(BaseModel):
    """Delta Table optimize specification."""

    z_order: list[str] | None = Field(default=None)
    vacuum: int | None = Field(default=None)
    compact: bool | None = Field(default=None)


class ColumnSourceTargetPair(BaseModel):
    """Column source and target pair."""

    column: str
    source: str


class DeltaTableMergeSpec(BaseModel):
    """Delta Table merge specification."""

    create_target_if_not_exists: bool | None = Field(default=True)
    condition: str
    when_matched: list[ColumnSourceTargetPair] | dict[str, str] | None = Field(
        default=None
    )
    when_not_matched: list[ColumnSourceTargetPair] | dict[str, str] | None = Field(
        default=None
    )
    when_not_matched_by_source: Literal["delete"] | list[ColumnSourceTargetPair] | dict[
        str, str
    ] | None = Field(default=None)


# The following classes are used to define the input and output specifications for the DeltaTable.
class DeltaTableOutputParamSpec(OutputBaseParamSpec):
    """Delta Table output parameters."""

    table: str | None = Field(default=None)
    location: str | None = Field(default=None)
    mode: str | None = Field(default=DeltaTableModeEnum.APPEND)
    partition_by: list[str] | None = Field(default=None)
    optimize: DeltaTableOptimizeSpec | None = Field(default=None, alias="optimise")
    merge_spec: DeltaTableMergeSpec | None = Field(default=None)

    @model_validator(mode="after")
    def validate_merge_spec(self):
        """Validates the merge specification."""
        if self.mode == DeltaTableModeEnum.MERGE and self.merge_spec is None:
            raise ValueError("Merge specification is required when mode is 'merge'.")

        return self


class DeltaTableOutputSpec(OutputBaseSpec):
    """Delta Table output specification."""

    type: Literal[IOEnum.DELTA_TABLE]
    params: DeltaTableOutputParamSpec


class DeltaTableInputParamSpec(InputBaseParamSpec):
    """Delta Table input parameters."""

    table: str | None = Field(default=None)
    location: str | None = Field(default=None)


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

        reader = self._spark.read.options(**spec.params.options)

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
        if spec.params.mode == DeltaTableModeEnum.MERGE and table_exists:
            # Perform a merge operation
            self.merge_into_table(spec, input_df)

        elif (
            spec.params.mode == DeltaTableModeEnum.MERGE
            and not table_exists
            and spec.params.merge_spec.create_target_if_not_exists is False
        ):
            raise ValueError("Table does not exist. Cannot perform merge operation.")
        else:
            if spec.params.mode == DeltaTableModeEnum.MERGE:
                spec.params.mode = DeltaTableModeEnum.OVERWRITE

            writer = (
                input_df.write.format("delta")
                .options(**spec.params.options)
                .mode(spec.params.mode)
            )

            if spec.params.partition_by is not None:
                writer = writer.partitionBy(**spec.params.partition_by)

            if spec.params.table is not None:
                writer.saveAsTable(spec.params.table)
            else:
                writer.save(spec.params.location)

        self.optimize_table(spec)
