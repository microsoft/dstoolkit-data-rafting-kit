from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, model_validator

from data_rafting_kit.data_quality.data_quality_spec import DataQualityRootSpec
from data_rafting_kit.io.io_spec import InputRootSpec, OutputRootSpec
from data_rafting_kit.testing.testing_spec import TestingRootSpec
from data_rafting_kit.transformations.transformation_spec import TransformationRootSpec


class TargetEnum(StrEnum):
    """Enum for target platforms."""

    FABRIC = "fabric"
    DATABRICKS = "databricks"
    LOCAL = "local"


class SecretSpec(BaseModel):
    """Secret specification. Used to specify the secret storage and key vault URI."""

    secret_storage: TargetEnum
    key_vault_uri: str | None = Field(default=None)

    @model_validator(mode="after")
    def validate_secret_spec(self):
        """Validates the secret spec."""
        if self.secret_storage == TargetEnum.FABRIC and self.key_vault_uri is None:
            raise ValueError("Key vault URI is required for fabric secret storage.")

        return self


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


class PipelineSpec(BaseModel):
    """Pipeline specification. Used to specify the inputs, outputs, and transformations for the pipeline."""

    inputs: list[InputRootSpec]
    outputs: list[OutputRootSpec] | None = Field(default_factory=list)
    transformations: list[TransformationRootSpec] | None = Field(default_factory=list)
    data_quality: list[DataQualityRootSpec] | None = Field(default_factory=list)


class ConfigurationSpec(BaseModel):
    """Data pipeline specification. This is the top-level specification for a data pipeline."""

    env: EnvSpec
    pipeline: PipelineSpec
    tests: TestingRootSpec | None = Field(default=None)

    model_config = ConfigDict(
        strict=True,
        validate_default=True,
        extra_values="forbid",
    )
