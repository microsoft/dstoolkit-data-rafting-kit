import inspect

from data_rafting_kit.common.base_factory import BaseFactory
from data_rafting_kit.transformations.pyspark import PYSPARK_DYNAMIC_TRANSFORMATIONS
from data_rafting_kit.transformations.transformation_mapping import (
    TransformationMapping,
)
from data_rafting_kit.transformations.transformation_spec import (
    PYSPARK_DYNAMIC_TRANSFORMATIONS_PARAMATER_REPLACEMENT_MAP,
    TransformationBaseSpec,
)


class TransformationFactory(BaseFactory):
    """Represents a Transformation Factory object for data pipelines."""

    def process_transformation(self, spec: TransformationBaseSpec):
        """Processes the transformation specification.

        Args:
        ----
            spec (TransformationBaseSpec): The transformation specification to process.
        """
        # Automatically use the last DataFrame if no input DataFrame is specified
        if spec.input_df is not None:
            input_df = self._dfs[spec.input_df]
        else:
            input_df = list(self._dfs.values())[-1]

        if spec.type in PYSPARK_DYNAMIC_TRANSFORMATIONS:
            transformation_function = TransformationMapping.get_transformation_map(
                spec.type, df=input_df
            )[0]

            # Adjust parameter names based on the replacement map
            params = spec.params.model_dump(by_alias=False)
            if spec.type in PYSPARK_DYNAMIC_TRANSFORMATIONS_PARAMATER_REPLACEMENT_MAP:
                replacement_map = (
                    PYSPARK_DYNAMIC_TRANSFORMATIONS_PARAMATER_REPLACEMENT_MAP[spec.type]
                )
                for original, replacement in replacement_map.items():
                    if original in params:
                        params[replacement] = params.pop(original)

            # Determine if the function expects positional or keyword arguments
            sig = inspect.signature(transformation_function)
            if any(
                param.kind == param.VAR_POSITIONAL for param in sig.parameters.values()
            ):
                # Handle positional arguments
                df = transformation_function(*params.values())
            else:
                # Handle keyword arguments
                df = transformation_function(**params)
        else:
            (
                transformation_class,
                transformation_function,
            ) = TransformationMapping.get_transformation_map(spec.type)
            df = getattr(
                transformation_class(self._spark, self.logger, self._dfs),
                transformation_function.__name__,
            )(spec, input_df)

        self._dfs[spec.name] = df
