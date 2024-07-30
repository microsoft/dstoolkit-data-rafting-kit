# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
import os

import yaml
from jinja2 import Template
from pydantic import BaseModel, ConfigDict, Field, RootModel, model_validator


def load_sub_config_file(data: dict) -> dict:
    """Loads a config file and renders it with the given parameters."""
    try:
        file_path = data["path"]
        arguments = data.get("arguments", {})

        with open(file_path, encoding="utf-8") as config_file:
            rendered_config = Template(config_file.read()).render(arguments)

            _, file_extension = os.path.splitext(file_path)
            if file_extension in (".yaml", ".yml"):
                config = yaml.safe_load(rendered_config)
            elif file_extension in ("json",):
                config = json.loads(rendered_config)
            else:
                raise ValueError(
                    f"Invalid config file at {file_path}. The config file for a parameter must be a YAML (.yaml or .yml) or JSON (.json) file."
                )

            if isinstance(config, dict):
                return config
            else:
                raise ValueError(
                    f"Invalid config file at {file_path}. The config file for a parameter must be a dictionary."
                )
    except KeyError:
        raise ValueError("Field required: path") from None
    except FileNotFoundError:
        raise ValueError(
            f"Config file not found at {file_path}. Check path to subconfig file is valid."
        ) from None


class ConfigParamSpec(BaseModel):
    """Base spec for class that references an external config file."""

    path: str
    arguments: dict | None = Field(default_factory=dict)

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

    config: ConfigParamSpec | None = Field(default=None, exclude=True)

    @model_validator(mode="before")
    @classmethod
    def validate_and_load_submodule(cls, data: dict) -> dict:
        """Validates the parameter spec."""
        if data is not None and "config" in data and data["config"] is not None:
            # Load the submodul
            loaded_config = load_sub_config_file(data["config"])

            if isinstance(loaded_config, dict):
                data.update(loaded_config)
                del data["config"]
            else:
                file_path = data["config"]["path"]
                raise ValueError(
                    f"Invalid config file at {file_path}. The config file for a parameter load into a dictionary."
                )
        return data


class BaseSpec(BaseModel):
    """Base class for all spec classes."""

    name: str
    type: str
    params: BaseParamSpec | None = Field(default_factory=BaseParamSpec)

    model_config = ConfigDict(
        validate_default=True, extra_values="forbid", validate_assignment=True
    )


class BaseRootModel(RootModel):
    """Base spec for class that references an external config file."""

    model_config = ConfigDict(
        validate_default=True, extra_values="forbid", validate_assignment=True
    )
