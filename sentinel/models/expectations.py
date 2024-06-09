from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from sentinel.models.metrics import MetricSet


class Expectations(BaseModel):

    expectation_type: Optional[str] = Field(None, alias='expectationType')
    kwargs: Optional[Dict[str, Any]] = None
    level: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = None

    class Config:
        arbitrary_types_allowed = True


class CustomExpectation(BaseModel):
    name: Optional[str] = None
    sql: Optional[str] = None
    level: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = None

    class Config:
        arbitrary_types_allowed = True


class DataQualityConfig(BaseModel):
    great_expectations: Optional[List[Expectations]] = Field(
        None, alias='greatExpectations'
    )
    custom_expectations: Optional[List[CustomExpectation]] = Field(
        None, alias='customExpectations'
    )
    metrics_config: List[MetricSet] = Field(default_factory=list)
