# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import os

from pydantic import BaseModel

from data_rafting_kit.env_spec import EnvSpec, TargetEnum


class Secret(BaseModel):
    """Secret Holder class. Holds the secret to prevent accidental exposure. Need to account for environmental variables."""

    name: str
    value: str

    def __str__(self):
        """Return the string representation of the Secret Holder Helper. Override to prevent the value from being printed."""
        return f"SecretHolderHelper: name={self.name}>"

    @classmethod
    def fetch_from_databricks(cls, spark, name: str):
        """Fetch the secret from the Databricks secret store.

        Args:
        ----
            spark (SparkSession): The SparkSession object.
            name (str): The name of the secret to fetch.

        Returns:
        -------
        Secret: The secret object.
        """
        try:
            from pyspark.dbutils import DBUtils

            db_utils = DBUtils(spark)

            return Secret(name=name, value=db_utils.secrets.get(name))
        except ImportError as e:
            raise ImportError(
                "DBUtils not available. Please run this on a Databricks cluster."
            ) from e

    @classmethod
    def fetch_from_local(cls, name: str):
        """Fetch the secret from the local environment.

        Args:
        ----
            name (str): The name of the secret to fetch.

        Returns:
        -------
        Secret: The secret object.
        """
        return Secret(name=name, value=os.getenv(name))

    @classmethod
    def fetch_from_fabric(cls, key_vault_uri: str, name: str):
        """Fetch the secret from the fabric.

        Args:
        ----
            key_vault_uri (str): The URI of the key vault.
            name (str): The name of the secret to fetch.

        Returns:
        -------
        Secret: The secret object.
        """
        try:
            from notebookutils import mssparkutils

            return Secret(
                name=name, value=mssparkutils.credentials.getSecret(key_vault_uri, name)
            )

        except ImportError as e:
            raise ImportError(
                "mssparkutils not available. Please run this on a Databricks cluster."
            ) from e

    @classmethod
    def fetch(cls, spark, env: EnvSpec, name: str):
        """Fetch the secret from the environment.

        Args:
        ----
            spark (SparkSession): The SparkSession object.
            env (EnvSpec): The environment specification.
            name (str): The name of the secret to fetch.

        Returns:
        -------
        Secret: The secret object.
        """
        if env.secrets.secret_storage == TargetEnum.LOCAL:
            return cls.fetch_from_local(name)
        elif env.secrets.secret_storage == TargetEnum.FABRIC:
            return cls.fetch_from_fabric(env.secrets.key_vault_uri, name)
        elif env.secrets.secret_storage == TargetEnum.DATABRICKS:
            return cls.fetch_from_databricks(spark, name)
