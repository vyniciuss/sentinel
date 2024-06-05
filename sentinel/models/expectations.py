from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class Expectations(BaseModel):

    expectation_type: Optional[str] = Field(None, alias='expectationType')
    kwargs: Optional[Dict[str, Any]] = None

    class Config:
        arbitrary_types_allowed = True


class CustomExpectation(BaseModel):
    name: Optional[str] = None
    sql: Optional[str] = None

    class Config:
        arbitrary_types_allowed = True


class DataQualityConfig(BaseModel):
    great_expectations: Optional[List[Expectations]] = Field(
        None, alias='greatExpectations'
    )
    custom_expectations: Optional[List[CustomExpectation]] = Field(
        None, alias='customExpectations'
    )
