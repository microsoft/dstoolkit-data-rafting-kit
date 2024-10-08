# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.


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
        input_df = self._dfs.last_df

        (
            data_quality_class,
            data_quality_function,
        ) = DataQualityMapping.get_data_quality_map(spec.type)

        if input_df.isStreaming:
            df = input_df.foreachBatch(
                lambda batch_df, _: getattr(
                    data_quality_class(
                        self._spark, self._logger, self._dfs, self._env, self._run_id
                    ),
                    data_quality_function.__name__,
                )(spec, batch_df),
            )
        else:
            df = getattr(
                data_quality_class(
                    self._spark, self._logger, self._dfs, self._env, self._run_id
                ),
                data_quality_function.__name__,
            )(spec, input_df)

        if isinstance(df, tuple):
            self._dfs[f"{spec.name}_fails"] = df[1]

            self._dfs[spec.name] = df[0]
        else:
            self._dfs[spec.name] = df
