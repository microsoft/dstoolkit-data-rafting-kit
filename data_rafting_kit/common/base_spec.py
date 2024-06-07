from pydantic import BaseModel


class BaseSpec(BaseModel):
    """Base class for all spec classes."""

    name: str
