from data_rafting_kit.common.base_factory import BaseFactory
from data_rafting_kit.data_quality.data_quality_base import (
    DataQualityBaseSpec,
)
from data_rafting_kit.data_quality.data_quality_mapping import (
    DataQualityMapping,
)


class DataQualityFactory(BaseFactory):
    """Represents a Data Quality Expectations Factory object for data pipelines."""

    def process_data_quality(self, spec: DataQualityBaseSpec):
        """Processes the data quality expectation specification.

        Args:
        ----
            spec (DataQualityBaseSpec): The data quality expectation specification to process.
        """
        # Automatically use the last DataFrame if no input DataFrame is specified
        if spec.input_df is not None:
            input_df = self._dfs[spec.input_df]
        else:
            input_df = list(self._dfs.values())[-1]

        (
            data_quality_class,
            data_quality_function,
        ) = DataQualityMapping.get_data_quality_map("great_expectations")

        df = getattr(
            data_quality_class(self._spark, self._logger, self._dfs, self._env),
            data_quality_function.__name__,
        )(spec, input_df)

        if isinstance(df, tuple):
            self._dfs[f"{spec.name}_fails"] = df[1]

            self._dfs[spec.name] = df[0]
        else:
            self._dfs[spec.name] = df
