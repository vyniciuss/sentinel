from typing import List, Optional

from pydantic import BaseModel


class MetricCondition(BaseModel):
    column_name: str
    condition: str


class MetricsConfig(BaseModel):
    accuracy: Optional[List[MetricCondition]]
    completeness: Optional[List[MetricCondition]]
    consistency: Optional[List[MetricCondition]]
    timeliness: Optional[List[MetricCondition]]
    validity: Optional[List[MetricCondition]]
