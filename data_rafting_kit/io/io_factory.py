import pyspark.sql.types as t
from pyspark.errors import PySparkAssertionError
from pyspark.testing import assertSchemaEqual

from data_rafting_kit.common.base_factory import BaseFactory
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
            fields = []
            for field in spec.params.expected_schema:
                pyspark_type_name = f"{field.type.capitalize()}Type"
                fields.append(
                    t.StructField(
                        field.name, getattr(t, pyspark_type_name)(), field.nullable
                    )
                )

            expected_schema = t.StructType(fields)

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
        if spec.input_df is not None:
            input_df = self._dfs[spec.input_df]
        else:
            input_df = list(self._dfs.values())[-1]

        output_class, output_function = IOMapping.get_output_map(spec.type)

        io_object = output_class(self._spark, self._logger, self._env)

        if spec.params.expected_schema is not None:
            self.validate_schema(spec, input_df)

        writer = getattr(io_object, output_function.__name__)(spec, input_df)

        if writer is not None and spec.params.streaming is not None:
            writer = writer.start()

            if spec.params.streaming.await_termination:
                writer.awaitTermination()
