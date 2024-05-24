from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class Expectations(BaseModel):

    expectation_type: Optional[str] = None
    expectation_type: Optional[str] = None
    kwargs: Optional[Dict[str, any]] = None

    class Config:
        arbitrary_types_allowed = True


class CustomExpectation(BaseModel):
    name: Optional[str] = None
    sql: Optional[str] = None
    result: Optional[any] = None

    class Config:
        arbitrary_types_allowed = True


class DataQualityConfig(BaseModel):
    great_expectations: Optional[List[Expectations]] = Field(
        None, alias='great_expectations'
    )
    custom_expectations: Optional[List[CustomExpectation]] = Field(
        None, alias='custom_expectations'
    )
