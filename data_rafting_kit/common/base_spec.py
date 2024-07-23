# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import yaml
from jinja2 import Template
from pydantic import BaseModel, ConfigDict, Field, RootModel, model_validator


class BaseSpec(BaseModel):
    """Base class for all spec classes."""

    name: str

    model_config = ConfigDict(
        validate_default=True, extra_values="forbid", validate_assignment=True
    )


class BaseRootModel(RootModel):
    """Base spec for class that references an external config file."""

    pass

    model_config = ConfigDict(
        validate_default=True, extra_values="forbid", validate_assignment=True
    )


class ConfigParamSpec(BaseModel):
    """Base spec for class that references an external config file."""

    path: str
    params: dict | None = Field(default_factory=dict)

    model_config = ConfigDict(
        validate_default=True, extra_values="forbid", validate_assignment=True
    )


class BaseParamSpec(BaseModel):
    """Base class for all parameter spec classes."""

    model_config = ConfigDict(
        validate_default=True,
        extra_values="forbid",
        populate_by_name=True,
        validate_assignment=True,
    )

    config: ConfigParamSpec | None = Field(default=None)

    @model_validator(mode="after")
    def validate_and_load_submodule(self, data: dict) -> dict:
        """Validates the parameter spec."""
        if hasattr(self, "config"):
            # Load the submodule
            with open(self.config.file_path, encoding="utf-8") as config_file:
                config = yaml.safe_load(
                    Template(config_file.read()).render(self.config.params)
                )

                if isinstance(config, dict):
                    data.update(config)
                    del data["config"]
                else:
                    raise ValueError(
                        f"Invalid config file at {self.config.file_path}. The config file for a parameter must be a dictionary."
                    )

        return data
