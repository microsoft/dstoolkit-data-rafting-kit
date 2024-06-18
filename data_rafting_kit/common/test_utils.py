import re

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="function")
def spark_session():
    """Fixture to set up Spark session for all tests."""
    spark = (
        SparkSession.builder.appName("DataQualityTests")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def logger():
    """Fixture to set up a fake logger for all tests."""

    class FakeLogger:
        def __init__(self):
            self.logs = []

        def info(self, message):
            self.logs.append(f"INFO: {message}")

        def error(self, message):
            self.logs.append(f"ERROR: {message}")

        def get_logs(self):
            return self.logs

    fake_logger = FakeLogger()
    yield fake_logger


def extract_and_convert_model_name_to_file_name(
    input_string, pattern
) -> tuple[str, str]:
    """Extract and convert the model name to a directory name and file name.

    Args:
    ----
        input_string (object): The input model name.
        pattern (str): The pattern to match.

    Returns:
    -------
        tuple[str, str]: The directory name and file name.
    """
    # Search for the pattern in the input string
    match = re.search(pattern, input_string)

    assert (
        match is not None
    ), f"Model name {input_string} does not match the expected pattern."

    # Convert to lowercase and replace camel case with underscores
    # First, insert underscores before each uppercase letter (except the first one)
    converted_file_name = re.sub(r"(?<!^)(?=[A-Z])", "_", match.group(2)).lower()

    converted_directory_name = re.sub(r"(?<!^)(?=[A-Z])", "_", match.group(1)).lower()

    return converted_directory_name, converted_file_name
