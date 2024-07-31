# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from enum import StrEnum
from typing import Literal

import pandas as pd
import pyspark.sql.functions as f
import pyspark.sql.types as t
from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig
from pydantic import Field, RootModel
from pyspark.sql import DataFrame

from data_rafting_kit.common.base_spec import BaseParamSpec
from data_rafting_kit.transformations.transformation_base import (
    TransformationBase,
    TransformationBaseSpec,
    TransformationEnum,
)


class PresidoOperatorEnum(StrEnum):
    """Enumeration class for Presido operators."""

    REPLACE = "replace"
    REDACT = "redact"
    HASH = "hash"
    MASK = "mask"


class PresidoReplaceOperatorSpec(BaseParamSpec):
    """Presido Replace Operator Specification."""

    type: Literal[PresidoOperatorEnum.REPLACE]
    name: str
    new_value: str | None = Field(default=None)
    entire_value: bool = False


class PresidoRedactOperatorSpec(BaseParamSpec):
    """Presido Redact Operator Specification."""

    type: Literal[PresidoOperatorEnum.REDACT]
    name: str
    entire_value: bool = False


class PresidoHashOperatorSpec(BaseParamSpec):
    """Presido Hash Operator Specification."""

    type: Literal[PresidoOperatorEnum.HASH]
    name: str
    hash_type: str | None = Field(default="sha256")


class PresidoMaskOperatorSpec(BaseParamSpec):
    """Presido Mask Operator Specification."""

    type: Literal[PresidoOperatorEnum.MASK]
    name: str
    chars_to_mask: int
    from_end: bool = False
    masking_char: str | None = Field(default="*")


class PresidoOperatorSpec(RootModel):
    """Presido Operator Specification."""

    root: PresidoReplaceOperatorSpec | PresidoRedactOperatorSpec | PresidoHashOperatorSpec | PresidoMaskOperatorSpec = Field(
        ..., discriminator="type"
    )


class PresidoAnonymizeTransformationParamSpec(BaseParamSpec):
    """PySpark With Columns Transformation Parameters."""

    columns: list[PresidoOperatorSpec]


class PresidoAnonymizeTransformationSpec(TransformationBaseSpec):
    """PySpark Join transformation specification."""

    type: Literal[TransformationEnum.ANONYMIZE]
    params: PresidoAnonymizeTransformationParamSpec


PRESIDO_TRANSFORMATION_SPECS = [
    PresidoAnonymizeTransformationSpec,
]


class PresidoTransformation(TransformationBase):
    """Represents a Presido transformation object for data pipelines."""

    def setup_presido(self) -> tuple:
        """Set up the Presido engine.

        Returns
        -------
            tuple: The tuple containing the broadcasted analyzer and anonymizer objects.
        """
        analyzer = AnalyzerEngine()
        anonymizer = AnonymizerEngine()
        broadcasted_analyzer = self._spark.sparkContext.broadcast(analyzer)
        broadcasted_anonymizer = self._spark.sparkContext.broadcast(anonymizer)

        return broadcasted_analyzer, broadcasted_anonymizer

    def anonymize(
        self, spec: PresidoAnonymizeTransformationSpec, input_df: DataFrame
    ) -> DataFrame:
        """Anonymize columns in a DataFrame according to the given spec.

        Args:
        ----
            spec (PresidoAnonymizeTransformationSpec): The Presido anonymize transformation specification.
            input_df (DataFrame): The input DataFrame.

        Returns:
        -------
            DataFrame: The resulting DataFrame.

        """
        self._logger.info("Anonymizing data...")

        broadcasted_analyzer, broadcasted_anonymizer = self.setup_presido()

        with_columns_map = {}

        for column in spec.params.columns:
            if (
                column.root.type
                in [PresidoOperatorEnum.REDACT, PresidoOperatorEnum.REPLACE]
                and column.root.entire_value
            ):
                if column.root.type == PresidoOperatorEnum.REDACT:
                    with_columns_map[column.root.name] = f.lit("[REDACTED]")
                else:
                    with_columns_map[column.root.name] = f.lit(column.root.new_value)
            else:

                def anonymized_udf(spec: PresidoOperatorSpec) -> object:
                    """Create a UDF for anonymizing a column according to the given spec.

                    Args:
                    ----
                        spec (PresidoOperatorSpec): The Presido operator specification.

                    Returns:
                    -------
                    object: The UDF function.
                    """
                    operator_params = spec.model_dump()

                    def anonymize_text(text: str) -> str:
                        """Anonymize a text according to the given spec.

                        Args:
                        ----
                            text (str): The text to anonymize.

                        Returns:
                        -------
                        str: The anonymized text.
                        """
                        if text is None:
                            return text

                        analyzer = broadcasted_analyzer.value
                        anonymizer = broadcasted_anonymizer.value
                        analyzer_results = analyzer.analyze(text=text, language="en")

                        # Anonymize the text with the operator config
                        anonymized_results = anonymizer.anonymize(
                            text=text,
                            analyzer_results=analyzer_results,
                            operators={
                                "DEFAULT": OperatorConfig(spec.type, operator_params)
                            },
                        )
                        return anonymized_results.text

                    def anonymize_series(s: pd.Series) -> pd.Series:
                        return s.apply(anonymize_text)

                    return f.pandas_udf(anonymize_series, returnType=t.StringType())

                with_columns_map[column.root.name] = anonymized_udf(column.root)(
                    f.col(column.root.name)
                )

        return input_df.withColumns(with_columns_map)
