from typing import Annotated, Union

from pydantic import Field, RootModel, create_model

from data_rafting_kit.common.base_spec import BaseSpec
from data_rafting_kit.data_quality.great_expectations import (
    GREAT_EXPECTATIONS_DATA_QUALITY_SPECS,
    DataQualityModeEnum,
)

ALL_DATA_QUALITY_SPECS = GREAT_EXPECTATIONS_DATA_QUALITY_SPECS

DataQualityCheckRootSpec = create_model(
    "DataQualityCheckRootSpec",
    root=Annotated[
        Union[tuple(ALL_DATA_QUALITY_SPECS)],
        Field(..., discriminator="type"),
    ],
    __base__=RootModel,
)

fields = {
    "input_df": Annotated[str | None, Field(default=None)],
    "checks": Annotated[list[DataQualityCheckRootSpec], Field(...)],
    "mode": Annotated[
        DataQualityModeEnum | None, Field(default=DataQualityModeEnum.FAIL)
    ],
    "unique_column_identifiers": Annotated[
        list[str] | None, Field(default_factory=list)
    ],
}
DataQualityRootSpec = create_model("DataQualityRootSpec", **fields, __base__=BaseSpec)
