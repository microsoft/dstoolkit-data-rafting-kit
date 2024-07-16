# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from pyspark.errors import PySparkAssertionError
from pyspark.testing import assertSchemaEqual

from data_rafting_kit.common.base_factory import BaseFactory
from data_rafting_kit.common.schema import to_pyspark_schema
from data_rafting_kit.io.io_base import InputBaseSpec, OutputBaseSpec
from data_rafting_kit.io.io_mapping import IOMapping


class IOFactory(BaseFactory):
    """Represents an IO Factory object for data pipelines."""

    def validate_schema(self, spec: InputBaseSpec | OutputBaseSpec, df):
        """Validates the schema of the DataFrame.

        Args:
        ----
            spec (InputBaseSpec | OutputBaseSpec): The input or output specification object.
            df (DataFrame): The DataFrame object.

        Raises:
        ------
            ValueError: If the schema does not match.
        """
        if (
            spec.params.expected_schema is not None
            and len(spec.params.expected_schema) > 0
        ):
            expected_schema = to_pyspark_schema(spec.params.expected_schema)

            try:
                assertSchemaEqual(actual=df.schema, expected=expected_schema)
            except PySparkAssertionError as e:
                self._logger.error("Schema Mismatch Found: %s", e)
                raise ValueError(
                    "Schema Mismatch Found. Unable to proceed with pipeline."
                ) from None

    def process_input(self, spec: InputBaseSpec):
        """Processes the input specification.

        Args:
        ----
            spec (InputBaseSpec): The input specification object.
        """
        input_class, input_function = IOMapping.get_input_map(spec.type)

        io_object = input_class(self._spark, self._logger, self._env)
        df = getattr(io_object, input_function.__name__)(spec)

        if spec.params.expected_schema is not None:
            self.validate_schema(spec, df)

        self._dfs[spec.name] = df

    def process_output(self, spec: OutputBaseSpec):
        """Processes the output specification.

        Args:
        ----
        spec (OutputBaseSpec): The out specification object.
        """
        # Automatically use the last DataFrame if no input DataFrame is specified
        input_df = self._dfs.get_df(spec.input_df)

        output_class, output_function = IOMapping.get_output_map(spec.type)

        io_object = output_class(self._spark, self._logger, self._env)

        if spec.params.expected_schema is not None:
            self.validate_schema(spec, input_df)

        writer = getattr(io_object, output_function.__name__)(spec, input_df)

        if writer is not None and spec.params.streaming is not None:
            writer = writer.option(
                "checkpointLocation", spec.params.streaming.checkpoint
            )

            if spec.params.streaming.trigger is not None:
                writer = writer.trigger(**spec.params.streaming.trigger)

            writer = writer.start()

            if spec.params.streaming.await_termination:
                writer.awaitTermination()
        elif writer is not None and spec.params.streaming is None:
            writer.save()

    def process_optimisation(self, spec: OutputBaseSpec):
        """Processes the optimisation specification.

        Args:
        ----
        spec (OutputBaseSpec): The output specification object.
        """
        output_class, output_function = IOMapping.get_optimisation_map(spec.type)

        if output_class is not None and output_function is not None:
            io_object = output_class(self._spark, self._logger, self._env)
            getattr(io_object, output_function.__name__)(spec)
