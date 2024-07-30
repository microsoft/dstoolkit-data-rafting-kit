# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from typing import Literal

import pyspark.sql.functions as f
from pydantic import Field
from pyspark.sql import DataFrame

from data_rafting_kit.io.io_base import (
    IOBase,
    IOEnum,
    OutputBaseParamSpec,
    OutputBaseSpec,
)


class ConsoleOutputParamSpec(OutputBaseParamSpec):
    """Console output parameters."""

    n: int | None = Field(default=5)
    truncate: bool | int | None = Field(default=False)
    vertical: bool | None = Field(default=False)


class ConsoleOutputSpec(OutputBaseSpec):
    """Console output specification."""

    type: Literal[IOEnum.CONSOLE]
    params: ConsoleOutputParamSpec | None = Field(
        default_factory=ConsoleOutputParamSpec
    )


class ConsoleIO(IOBase):
    """Represents a Console object for data pipelines."""

    def write(self, spec: ConsoleOutputSpec, input_df: DataFrame):
        """Writes to a Console on the Console system.

        Args:
        ----
            spec (ConsoleOutputSpec): The output parameter specification object.
            input_df (DataFrame): The DataFrame object to write.
        """
        self._logger.info("Writing to Console...")

        if spec.params.streaming is not None:
            writer = input_df.writeStream

            if spec.params.streaming.trigger is not None:
                writer = writer.trigger(**spec.params.streaming.trigger)

            def foreach_batch_function(df, epoch_id):
                self._logger.info("Processing epoch %s ...", epoch_id)
                df.withColumn("epoch", f.lit(epoch_id)).show(
                    n=spec.params.n,
                    truncate=spec.params.truncate,
                    vertical=spec.params.vertical,
                )

            writer = writer.foreachBatch(foreach_batch_function).start()

            if spec.params.streaming.await_termination:
                writer = writer.awaitTermination()
        else:
            input_df.show(
                n=spec.params.n,
                truncate=spec.params.truncate,
                vertical=spec.params.vertical,
            )

            return None

        return writer
