import builtins
import inspect
import re
import typing
from typing import Annotated, Literal, Union

from pydantic import ConfigDict, Field, RootModel, create_model

from data_rafting_kit.transformations.presido import (
    PRESIDO_TRANSFORMATION_SPECS,
)
from data_rafting_kit.transformations.pyspark import (
    PYSPARK_DYNAMIC_TRANSFORMATIONS,
    PYSPARK_TRANSFORMATION_SPECS,
)
from data_rafting_kit.transformations.transformation_base import (
    TransformationBaseSpec,
)
from data_rafting_kit.transformations.transformation_mapping import (
    TransformationMapping,
)


def is_builtin(t: type) -> bool:
    """Check if a type is a builtin type.

    Args:
    ----
        t (type): The type to check.

    Returns:
    -------
    bool: True if the type is a builtin type, False otherwise.
    """
    return t.__name__ in dir(builtins)


def clean_type(t: type) -> type | None:
    """Clean a type to remove any typing annotations that aren't defaults.

    Args:
    ----
        t (type): The type to clean.

    Returns:
    -------
        type: The cleaned type.
    """
    origin = getattr(t, "__origin__", None)
    if origin is not None:
        if origin is typing.Union:
            args = [a for a in t.__args__ if is_builtin(a) or clean_type(a)]
            return (
                typing.Optional[args[0]]  # noqa: UP007
                if len(args) == 1
                else typing.Union[tuple(args)]  # noqa: UP007
                if args
                else None
            )
        elif origin is dict:
            key_type = clean_type(t.__args__[0])
            value_type = clean_type(t.__args__[1])
            return dict[key_type, value_type] if key_type and value_type else None
        elif origin is tuple:
            args = tuple(clean_type(a) for a in t.__args__)
            return tuple[args] if all(args) else None
        else:
            args = [clean_type(a) for a in t.__args__]
            return origin[tuple(args)]
    elif isinstance(t, str):
        return str
    elif is_builtin(t):
        return t


def is_type_optional(t: type) -> bool:
    """Check if a type is optional.

    Args:
    ----
        t (type): The type to check.

    Returns:
    -------
    bool: True if the type is optional, False otherwise.
    """
    origin = getattr(t, "__origin__", None)

    if origin is not None:
        if origin is typing.Union:
            return type(None) in t.__args__
        else:
            return False
    else:
        return False


PYSPARK_DYNAMIC_TRANSFORMATIONS_PARAMATER_REPLACEMENT_MAP = {
    "drop": {"cols": "column"},
    "with_columns_renamed": {"colsMap": "columns_map"},
}

dynamic_pyspark_transformation_models = []
for transformation in PYSPARK_DYNAMIC_TRANSFORMATIONS:
    param_fields = {}
    transformation_function = TransformationMapping.get_transformation_map(
        transformation
    )
    transformation_sig = inspect.signature(transformation_function[0])

    all_optional_fields = True
    for name, param in transformation_sig.parameters.items():
        if name != "self":
            cleaned_type = clean_type(param.annotation)
            is_optional = is_type_optional(param.annotation)

            if not is_optional:
                all_optional_fields = False

            try:
                cleaned_name = (
                    PYSPARK_DYNAMIC_TRANSFORMATIONS_PARAMATER_REPLACEMENT_MAP[
                        str(transformation)
                    ][name]
                )
            except KeyError:
                cleaned_name = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()

            if param.default is not inspect.Parameter.empty:
                param_fields[name] = (
                    cleaned_type,
                    Field(default=param.default, alias=cleaned_name),
                )
            else:
                param_fields[name] = (cleaned_type, Field(alias=cleaned_name))

    if len(param_fields) > 0:
        param_config = ConfigDict(allow_population_by_field_name=True)
        dynamic_transformation_param_model = create_model(
            f"{transformation}_params", **param_fields, __config__=param_config
        )

        if all_optional_fields:
            fields = {
                "type": Annotated[Literal[transformation], Field(...)],
                "params": Annotated[
                    dynamic_transformation_param_model | None,
                    Field(default_factory=dynamic_transformation_param_model),
                ],
            }
        else:
            fields = {
                "type": Annotated[Literal[transformation], Field(...)],
                "params": Annotated[dynamic_transformation_param_model, Field(...)],
            }

    else:
        fields = {
            "type": Annotated[Literal[transformation], Field(...)],
            "params": Annotated[None, Field(default_factory=dict)],
        }

    normalised_transformation_name = (
        transformation.replace("_", " ").title().replace(" ", "")
    )
    model_name = f"Pyspark{normalised_transformation_name}TransformationSpec"
    dynamic_transformation_model = create_model(
        model_name, **fields, __base__=TransformationBaseSpec
    )
    dynamic_pyspark_transformation_models.append(dynamic_transformation_model)

ALL_TRANSFORMATION_SPECS = (
    PRESIDO_TRANSFORMATION_SPECS
    + PYSPARK_TRANSFORMATION_SPECS
    + dynamic_pyspark_transformation_models
)
TransformationRootSpec = create_model(
    "TransformationRootSpec",
    root=Annotated[
        Union[tuple(ALL_TRANSFORMATION_SPECS)],  # noqa: UP007
        Field(..., discriminator="type"),
    ],
    __base__=RootModel,
)
