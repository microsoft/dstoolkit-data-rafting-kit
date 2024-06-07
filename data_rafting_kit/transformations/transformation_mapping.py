from pyspark.sql import DataFrame

from data_rafting_kit.transformations.presido import PresidoTransformation
from data_rafting_kit.transformations.pyspark import PysparkTransformation
from data_rafting_kit.transformations.transformation_base import TransformationEnum


class TransformationMapping:
    """Holds the mapping for the transformation types."""

    @staticmethod
    def get_transformation_map(key: TransformationEnum, df=None) -> object:
        """Maps the transformation type to the corresponding class.

        Returns
        -------
        dict: The dictionary mapping the transformation type to the corresponding class.
        """
        if df is None:
            df = DataFrame

        map = {
            TransformationEnum.AGG: (getattr(df, "agg", None),),
            TransformationEnum.ANONYMIZE: (
                PresidoTransformation,
                PresidoTransformation.anonymize,
            ),
            TransformationEnum.DISTINCT: (getattr(df, "distinct", None),),
            TransformationEnum.DROP: (getattr(df, "drop", None),),
            TransformationEnum.DROP_DUPLICATES: (getattr(df, "dropDuplicates", None),),
            TransformationEnum.FILTER: (getattr(df, "filter", None),),
            TransformationEnum.GROUP_BY: (getattr(df, "groupBy", None),),
            TransformationEnum.INTERSECT: (getattr(df, "intersect", None),),
            TransformationEnum.JOIN: (
                PysparkTransformation,
                PysparkTransformation.join,
            ),
            TransformationEnum.WITH_COLUMNS: (
                PysparkTransformation,
                PysparkTransformation.with_columns,
            ),
            TransformationEnum.WITH_COLUMNS_RENAMED: (
                getattr(df, "withColumnsRenamed", None),
            ),
        }

        if key not in map or map[key] is None:
            raise NotImplementedError(f"Transformation Type {key} not implemented")

        return map[key]
