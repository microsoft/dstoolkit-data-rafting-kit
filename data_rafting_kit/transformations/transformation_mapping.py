from typing import ClassVar

from pyspark.sql import DataFrame

from data_rafting_kit.transformations.presido import PresidoTransformation
from data_rafting_kit.transformations.pyspark import PysparkTransformation
from data_rafting_kit.transformations.transformation_base import TransformationEnum
from data_rafting_kit.transformations.window_transformations import (
    WindowTransformation,
)


class TransformationMapping:
    """Holds the mapping for the transformation types."""

    MAP: ClassVar[dict] = {
        TransformationEnum.AGG: (getattr(DataFrame, "agg", None),),
        TransformationEnum.ANONYMIZE: (
            PresidoTransformation,
            PresidoTransformation.anonymize,
        ),
        TransformationEnum.DISTINCT: (getattr(DataFrame, "distinct", None),),
        TransformationEnum.DROP: (getattr(DataFrame, "drop", None),),
        TransformationEnum.DROP_DUPLICATES: (
            getattr(DataFrame, "dropDuplicates", None),
        ),
        TransformationEnum.FILTER: (getattr(DataFrame, "filter", None),),
        TransformationEnum.GROUP_BY: (getattr(DataFrame, "groupBy", None),),
        TransformationEnum.INTERSECT: (getattr(DataFrame, "intersect", None),),
        TransformationEnum.JOIN: (
            PysparkTransformation,
            PysparkTransformation.join,
        ),
        TransformationEnum.WITH_COLUMNS: (
            PysparkTransformation,
            PysparkTransformation.with_columns,
        ),
        TransformationEnum.WITH_COLUMNS_RENAMED: (
            getattr(DataFrame, "withColumnsRenamed", None),
        ),
        TransformationEnum.WINDOW_FUNCTION: (
            WindowTransformation,
            WindowTransformation.apply_window_function,
        ),
    }

    @staticmethod
    def get_transformation_map(key: TransformationEnum, df=None) -> object:
        """Maps the transformation type to the corresponding class.

        Args:
        ----
            key (TransformationEnum): The transformation type key.
            df (DataFrame, optional): The DataFrame to be transformed.

        Returns:
        -------
            object: The transformation function or class.

        Raises:
        ------
            NotImplementedError: If the transformation type is not implemented.
        """
        if df is None:
            df = DataFrame

        if (
            key not in TransformationMapping.MAP
            or TransformationMapping.MAP[key] is None
        ):
            raise NotImplementedError(f"Transformation Type {key} not implemented")

        return TransformationMapping.MAP[key]
