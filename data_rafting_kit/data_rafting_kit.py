# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import logging
import os
from collections import OrderedDict

import yaml
from jinja2 import Template
from pydantic import ValidationError
from pyspark.errors import PySparkAssertionError
from pyspark.sql import DataFrame
from pyspark.testing import assertDataFrameEqual

from data_rafting_kit.configuration_spec import ConfigurationSpec
from data_rafting_kit.data_quality.data_quality_factory import DataQualityFactory
from data_rafting_kit.io.io_factory import IOFactory
from data_rafting_kit.transformations.transformation_factory import (
    TransformationFactory,
)


class DataRaftingKit:
    """The DataRaftingKit class is responsible for executing a data pipeline."""

    def __init__(self, spark, raw_data_pipeline_spec: dict, verbose: bool = False):
        """Initialize the DataPipeline object.

        Args:
        ----
            spark (SparkSession): The SparkSession object.
            raw_data_pipeline_spec (dict): The raw data pipeline specification.
            verbose (bool, optional): Whether to log verbose messages. Defaults to False.
        """
        self._spark = spark
        self._verbose = verbose

        self._logger = logging.getLogger(__name__)

        self._raw_data_pipeline_spec = raw_data_pipeline_spec

        self._data_pipeline_spec = None

        self._logger.info("Data Pipeline Config Loaded")

    @classmethod
    def from_yaml_str(
        cls,
        spark,
        yaml_str: str,
        params: dict | None = None,
        verbose: bool = False,
    ) -> object:
        """Load the test config from the config file.

        Args:
        ----
            spark (SparkSession): The SparkSession object.
            yaml_str (str): The string of YAML to load.
            params (dict, optional): The parameters to render the config file. Defaults to None.
            verbose (bool, optional): Whether to log verbose messages. Defaults to False.

        Returns:
        -------
        DataRaftingKit: The DataRaftingKit object.
        """
        raw_data_pipeline_spec = yaml.safe_load(Template(yaml_str).render(params))

        data_rafting_kit = cls(spark, raw_data_pipeline_spec, verbose)

        return data_rafting_kit

    @classmethod
    def from_yaml_file(
        cls,
        spark,
        yaml_file_path: str,
        params: dict | None = None,
        verbose: bool = False,
    ) -> object:
        """Load the test config from the config file.

        Args:
        ----
            spark (SparkSession): The SparkSession object.
            yaml_file_path (str): The path to the config file.
            params (dict, optional): The parameters to render the config file. Defaults to None.
            verbose (bool, optional): Whether to log verbose messages. Defaults to False.

        Returns:
        -------
        DataRaftingKit: The DataRaftingKit object.
        """
        if not os.path.isfile(yaml_file_path):
            raise FileNotFoundError(f"Config file not found at {yaml_file_path}")

        with open(yaml_file_path, encoding="utf-8") as config_file:
            return cls.from_yaml_str(spark, config_file.read(), params, verbose)

    def validate(self) -> bool:
        """Validate the data pipeline spec.

        Returnsll
        -------
            bool: True if the data pipeline spec is valid, False otherwise.
        """
        try:
            self._data_pipeline_spec = ConfigurationSpec.model_validate(
                self._raw_data_pipeline_spec
            )

            return True
        except ValidationError as e:
            self._logger.error("%i Validation Errors Found:", e.error_count())
            for error in e.errors():
                self._logger.error(
                    "Config Location: %s -> %s -> Found: %s",
                    error["loc"],
                    error["msg"],
                    error["input"],
                )
            return False

    def run_pipeline_from_spec(
        self, data_pipeline_spec: ConfigurationSpec, write_outputs: bool = True
    ) -> OrderedDict[str, DataFrame]:
        """Run a pipeline from a data pipeline spec.

        Args:
        ----
            data_pipeline_spec (ConfigurationSpec): The data pipeline specification.
            write_outputs (bool, optional): Whether to write the outputs. Defaults to True.

        Returns:
        -------
            OrderedDict[str, DataFrame]: The final DataFrames in the pipeline.
        """
        dfs = OrderedDict()
        self._logger.info("Executing Data Pipeline")

        io_factory = IOFactory(self._spark, self._logger, dfs, data_pipeline_spec.env)

        for input_spec in data_pipeline_spec.pipeline.inputs:
            self._logger.info("Reading from %s", input_spec.root.name)

            io_factory.process_input(input_spec.root)

        transformation_factory = TransformationFactory(
            self._spark, self._logger, dfs, data_pipeline_spec.env
        )
        for transformation_spec in data_pipeline_spec.pipeline.transformations:
            self._logger.info(
                "Applying transformation %s", transformation_spec.root.type
            )

            transformation_factory.process_transformation(transformation_spec.root)

        data_quality_factory = DataQualityFactory(
            self._spark, self._logger, dfs, data_pipeline_spec.env
        )
        for data_quality_check_spec in data_pipeline_spec.pipeline.data_quality:
            self._logger.info(
                "Applying data quality check %s", data_quality_check_spec.name
            )
            data_quality_factory.process_data_quality(data_quality_check_spec)

        if write_outputs:
            for output_spec in data_pipeline_spec.pipeline.outputs:
                self._logger.info("Writing to %s", output_spec.root.name)

                io_factory.process_output(output_spec.root)
                io_factory.process_optimisation(output_spec.root)

        self._logger.info("Data Pipeline Execution Complete")

        return dfs

    def test_local(self):  # noqa: PLR0912
        """Test the data pipeline locally with a spark installation."""
        if not self.validate():
            self._logger.error(
                "Testing of Data Pipeline Failed due to validation error."
            )
            return None

        if (
            self._data_pipeline_spec.tests is None
            or len(self._data_pipeline_spec.tests.local) == 0
        ):
            self._logger.info("No local tests found in the data pipeline spec")
            return None

        for test_spec in self._data_pipeline_spec.tests.local:
            test_pipeline_spec = self._data_pipeline_spec.model_copy(deep=True)
            self._logger.info("Testing Data Pipeline Locally")

            # Merge the test inputs into the data pipeline spec
            for input_index, input_spec in enumerate(
                test_pipeline_spec.pipeline.inputs
            ):
                for test_input_spec in test_spec.mock_inputs:
                    if input_spec.root.name == test_input_spec.root.name:
                        test_pipeline_spec.pipeline.inputs[
                            input_index
                        ] = input_spec.model_copy(
                            update=test_input_spec.model_dump(exclude_unset=True),
                            deep=True,
                        )

                        break

            dfs = self.run_pipeline_from_spec(test_pipeline_spec, write_outputs=False)

            expected_dfs = OrderedDict()

            io_factory = IOFactory(
                self._spark, self._logger, expected_dfs, test_pipeline_spec.env
            )

            # Analyse the outputs to determine if the test passed
            for expected_output_spec in test_spec.expected_outputs:
                for output_spec in test_pipeline_spec.pipeline.outputs:
                    if output_spec.root.name == expected_output_spec.root.name:
                        if output_spec.root.input_df is not None:
                            input_df = dfs[output_spec.root.input_df]
                        else:
                            input_df = list(dfs.values())[-1]

                        self._logger.info(
                            "Reading expected output from %s",
                            expected_output_spec.root.name,
                        )

                        io_factory.process_input(expected_output_spec.root)

                        try:
                            assertDataFrameEqual(
                                actual=input_df,
                                expected=expected_dfs[expected_output_spec.root.name],
                            )

                            break
                        except PySparkAssertionError as e:
                            self._logger.error("Mismatching Dataframe Found: %s", e)
                            raise ValueError(
                                f"Expected output did not match the expected input for output: {expected_output_spec.root.name}."
                            ) from None

    def execute(self) -> DataFrame:
        """Execute the data pipeline according to the data pipeline spec.

        Returns
        -------
            DataFrame: The final DataFrame object in the data pipeline.
        """
        if not self.validate():
            self._logger.error(
                "Data Pipeline Execution Failed due to validation error."
            )
            return None

        dfs = self.run_pipeline_from_spec(self._data_pipeline_spec)
        return list(dfs.values())[-1]
