# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class TargetEnum(StrEnum):
    """Enum for target platforms."""

    FABRIC = "fabric"
    DATABRICKS = "databricks"
    LOCAL = "local"


class SecretSpec(BaseModel):
    """Secret specification. Used to specify the secret storage and key vault URI."""

    secret_storage: Literal[TargetEnum.FABRIC, TargetEnum.DATABRICKS, TargetEnum.LOCAL]
    key_vault_uri: str | None = Field(default=None)

    @model_validator(mode="after")
    def validate_secret_spec(self):
        """Validates the secret spec."""
        if self.secret_storage == TargetEnum.FABRIC and self.key_vault_uri is None:
            raise ValueError("Key vault URI is required for fabric secret storage.")

        return self

    model_config = ConfigDict(
        strict=True,
        validate_default=True,
        extra_values="forbid",
    )


class EnvSpec(BaseModel):
    """Environment specification. Used to specify changes to the environment and config."""

    target: TargetEnum
    secrets: SecretSpec | None = Field(default=None)

    @model_validator(mode="after")
    def default_secrets_spec_creator(self):
        """Validates and adds default values for the secret storage."""
        if self.secrets is None:
            self.secrets = SecretSpec(secret_storage=self.target)

        return self

    model_config = ConfigDict(
        strict=True,
        validate_default=True,
        extra_values="forbid",
    )
