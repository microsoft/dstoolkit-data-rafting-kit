# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from enum import StrEnum
from typing import Literal

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pydantic import Field, model_validator
from pyspark.sql import DataFrame
from pyspark.sql.avro.functions import from_avro, to_avro

from data_rafting_kit.common.schema import (
    SchemaFieldSpec,
    to_pyspark_schema,
    to_pyspark_struct,
)
from data_rafting_kit.common.secret_management import Secret
from data_rafting_kit.io.io_base import (
    InputBaseParamSpec,
    InputBaseSpec,
    IOBase,
    IOEnum,
    OutputBaseParamSpec,
    OutputBaseSpec,
)


class SerializeDeserializeEnum(StrEnum):
    """Enumeration class for serialization and deserialization types."""

    JSON = "json"
    AVRO = "avro"


class EventHubOutputParamSpec(OutputBaseParamSpec):
    """EventHub output parameters."""

    namespace: str
    hub: str | None = Field(default=None)
    connection_string_key: str
    format: Literal[
        SerializeDeserializeEnum.AVRO, SerializeDeserializeEnum.JSON
    ] | None = Field(default=None)
    format_schema: list[SchemaFieldSpec] | None = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def validate_event_hub_output_param_spec_before(cls, data: dict) -> dict:
        """Validates the Delta Table output param spec."""
        if data["streaming"] is not None:
            if isinstance(data["streaming"], bool):
                data["streaming"] = {}

            if "checkpoint" not in data["streaming"]:
                data["streaming"][
                    "checkpoint"
                ] = f"/.checkpoints/event_hub/{data['namespace']}/{data['hub']}"

        return data

    @model_validator(mode="after")
    def validate_event_hub_output_spec_after(self):
        """Validates the EventHub output spec."""
        if self.format is not None and self.format_schema is None:
            raise ValueError("format_schema must be provided when format is provided.")

        return self


class EventHubOutputSpec(OutputBaseSpec):
    """EventHub output specification."""

    type: Literal[IOEnum.EVENT_HUB]
    params: EventHubOutputParamSpec


class EventHubInputParamSpec(InputBaseParamSpec):
    """EventHub input parameters."""

    namespace: str
    hub: str
    connection_string_key: str
    format: Literal[
        SerializeDeserializeEnum.AVRO, SerializeDeserializeEnum.JSON
    ] | None = Field(default=None)
    format_schema: list[SchemaFieldSpec] | None = Field(default=None)

    @model_validator(mode="after")
    def validate_event_hub_input_param_spec(self):
        """Validates the EventHub input spec."""
        if self.format is not None and self.format_schema is None:
            raise ValueError("format_schema must be provided when format is provided.")

        return self


class EventHubInputSpec(InputBaseSpec):
    """EventHub input specification."""

    type: Literal[IOEnum.EVENT_HUB]
    params: EventHubInputParamSpec


class EventHubIO(IOBase):
    """Represents a EventHub object for data pipelines."""

    def get_kafka_options(
        self, spec: EventHubInputParamSpec | EventHubOutputSpec
    ) -> dict:
        """Gets the Kafka options for the EventHub system.

        Args:
        ----
            spec (EventHubInputParamSpec | EventHubOutputSpec): The input or output parameter specification object.

        Returns:
        -------
        dict: The Kafka options.
        """
        options = {
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.request.timeout.ms": "60000",
            "kafka.session.timeout.ms": "60000",
        }

        options[
            "kafka.bootstrap.servers"
        ] = f"{spec.params.namespace}.servicebus.windows.net:9093"

        if spec.params.hub is not None:
            options["subscribe"] = spec.params.hub

        connection_string = Secret.fetch(
            self._spark, self._env, spec.params.connection_string_key
        )

        options[
            "kafka.sasl.jaas.config"
        ] = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{connection_string.value}";'

        return options

    def read(self, spec: EventHubInputSpec) -> DataFrame:
        """Reads from a EventHub on the EventHub system.

        Args:
        ----
            spec (EventHubInputSpec): The input parameter specification object.

        Returns:
        -------
            DataFrame: The DataFrame object.
        """
        self._logger.info("Reading from EventHub...")

        options = self.get_kafka_options(spec)
        options.update(spec.params.options)

        reader = self._spark.readStream if spec.params.streaming else self._spark.read

        stream = reader.format("kafka").options(**options).load()

        if spec.params.format is not None:
            schema = to_pyspark_schema(spec.params.format_schema)
            stream = stream.withColumn("value", f.col("value").cast(t.StringType()))

            if spec.params.format == SerializeDeserializeEnum.AVRO:
                stream = stream.withColumn("value", from_avro(f.col("value"), schema))
            elif spec.params.format == SerializeDeserializeEnum.JSON:
                stream = stream.withColumn("value", f.from_json(f.col("value"), schema))

            columns = [*stream.columns, "value.*"]
            columns.remove("value")
            stream = stream.select(columns)

        return stream

    def write(self, spec: EventHubOutputSpec, input_df: DataFrame):
        """Writes to a EventHub on the EventHub system.

        Args:
        ----
            spec (EventHubOutputSpec): The output parameter specification object.
            input_df (DataFrame): The DataFrame object to write.
        """
        self._logger.info("Writing to EventHub...")

        options = self.get_kafka_options(spec)
        options.update(spec.params.options)

        if spec.params.format is not None:
            schema = to_pyspark_struct(spec.params.format_schema)

            if spec.params.format == SerializeDeserializeEnum.AVRO:
                input_df = input_df.withColumn(
                    "value", to_avro(f.struct(input_df.columns), schema)
                )
            elif spec.params.format == SerializeDeserializeEnum.JSON:
                input_df = input_df.withColumn("value", f.to_json(schema))

        stream = input_df.withColumn("value", f.col("value").cast(t.BinaryType()))

        if spec.params.streaming is not None:
            writer = stream.writeStream.outputMode(spec.params.mode.value)
        else:
            writer = stream.write.mode(spec.params.mode)

        writer = writer.format("kafka").options(**options)

        return writer
