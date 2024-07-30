from collections import OrderedDict

from pyspark.sql import DataFrame


class PipelineDataframeHolder(OrderedDict):
    """Represents a Pipeline DataFrame Holder object for data pipelines."""

    @property
    def last_df(self):
        """Returns the last DataFrame in the ordered dictionary."""
        return list(self.values())[-1]

    @property
    def last_key(self):
        """Returns the last DataFrame key in the ordered dictionary."""
        return list(self.keys())[-1]

    def get_df(self, input_df: str | None) -> DataFrame:
        """Returns the DataFrame object based on the input DataFrame name.

        Args:
        ----
            input_df (str): The input DataFrame name.

        Returns:
        -------
            DataFrame: The DataFrame object.
        """
        if input_df is not None:
            return self[input_df]
        else:
            return self.last_df
