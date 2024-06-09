from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class MetricCondition(BaseModel):
    column_name: str
    condition: str


class ValidationRule(BaseModel):
    name: str
    condition: str
    error_message: str


class MetricSet(BaseModel):
    id: str
    name: str
    group_by: Optional[str] = None
    metrics: Dict[str, List[MetricCondition]]
    validation_rules: Optional[List[ValidationRule]] = Field(
        default_factory=list
    )
