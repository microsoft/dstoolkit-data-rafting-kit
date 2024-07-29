# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import inspect

from data_rafting_kit.common.base_factory import BaseFactory
from data_rafting_kit.transformations.pyspark import PYSPARK_DYNAMIC_TRANSFORMATIONS
from data_rafting_kit.transformations.transformation_mapping import (
    TransformationMapping,
)
from data_rafting_kit.transformations.transformation_spec import (
    TransformationBaseSpec,
)


class TransformationFactory(BaseFactory):
    """Processes transformations for data pipelines."""

    def process_transformation(self, spec: TransformationBaseSpec):
        """Processes the transformation specification.

        Args:
        ----
            spec (TransformationBaseSpec): The transformation specification to process.

        """
        input_df = self._dfs.get_df(spec.input_df)

        if spec.type in PYSPARK_DYNAMIC_TRANSFORMATIONS:
            transformation_function = TransformationMapping.get_transformation_map(
                spec.type, df=input_df
            )[0]

            if hasattr(spec, "params"):
                params = spec.params.model_dump(by_alias=False)
            else:
                params = {}

            sig = inspect.signature(transformation_function)
            if any(
                param.kind == param.VAR_POSITIONAL for param in sig.parameters.values()
            ):
                df = transformation_function(input_df, *params.values())
            else:
                df = transformation_function(input_df, **params)
        else:
            (
                transformation_class,
                transformation_function,
            ) = TransformationMapping.get_transformation_map(spec.type)
            df = getattr(
                transformation_class(self._spark, self._logger, self._dfs, self._env),
                transformation_function.__name__,
            )(spec, input_df)

        self._dfs[spec.name] = df
