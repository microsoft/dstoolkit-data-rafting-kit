from typing import Literal

from pydantic import Field, model_validator
from pyspark.sql import DataFrame

from data_rafting_kit.common.secret_management import Secret
from data_rafting_kit.io.io_base import (
    InputBaseParamSpec,
    InputBaseSpec,
    IOBase,
    IOEnum,
    OutputBaseParamSpec,
    OutputBaseSpec,
)


class EventHubOutputParamSpec(OutputBaseParamSpec):
    """EventHub output parameters."""

    namespace: str
    hub: str
    connection_string_key: str
    checkpoint_location: str | None = Field(default=None)

    @model_validator(mode="after")
    def validate_event_hub_output_spec(self):
        """Validates the EventHub output spec."""
        if self.checkpoint_location is None:
            self.checkpoint_location = f"{self.namespace}/{self.hub}"

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
        options["subscribe"] = spec.params.hub

        connection_string = Secret.fetch(
            self._spark, self._env, spec.params.connection_string_key
        )

        options[
            "kafka.sasl.jaas.config"
        ] = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{connection_string.value}";'

        if isinstance(spec, EventHubOutputSpec):
            options["checkpointLocation"] = spec.params.checkpoint_location

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

        return self._spark.readStream.format("kafka").options(**options).load()

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

        writer = (
            input_df.writeStream.format("kafka")
            .outputMode(spec.params.mode)
            .options(**options)
        )

        return writer
