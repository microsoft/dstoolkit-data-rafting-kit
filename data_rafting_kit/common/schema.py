# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from typing import Literal

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pydantic import Field

from data_rafting_kit.common.base_spec import BaseParamSpec


class SchemaFieldSpec(BaseParamSpec):
    """Schema field."""

    name: str
    type: (
        Literal[
            "string",
            "binary",
            "boolean",
            "date",
            "timestamp",
            "decimal",
            "double",
            "float",
            "byte",
            "integer",
            "long",
            "short",
            "array",
            "map",
        ]
        | None
    ) = Field(default="string")
    nullable: bool | None = Field(default=True)


def to_pyspark_struct(schema: list[SchemaFieldSpec]) -> f.struct:
    """Converts a schema field to a PySpark StructField.

    Args:
    ----
        schema (list[SchemaFieldSpec]): The schema.

    Returns:
    -------
        f.struct: The PySpark struct.
    """
    fields = []
    for field in schema:
        fields.append(field.name)

    return f.struct(fields)


def to_pyspark_schema(schema: list[SchemaFieldSpec]) -> t.StructType:
    """Converts the schema to a PySpark schema.

    Args:
    ----
        schema (list[SchemaFieldSpec]): The schema.

    Returns:
    -------
        t.StructType: The PySpark schema.
    """
    fields = []
    for field in schema:
        pyspark_type_name = f"{field.type.capitalize()}Type"
        fields.append(
            t.StructField(field.name, getattr(t, pyspark_type_name)(), field.nullable)
        )

    return t.StructType(fields)
