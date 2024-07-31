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


class ExpectationGroup(BaseModel):
    name: str
    expectations: List[Expectations]

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


class CustomExpectationGroup(BaseModel):
    name: str
    expectations: List[CustomExpectation]

    class Config:
        arbitrary_types_allowed = True


class DataQualityConfig(BaseModel):
    great_expectations: Optional[List[ExpectationGroup]] = Field(
        None, alias='greatExpectations'
    )
    custom_expectations: Optional[List[CustomExpectationGroup]] = Field(
        None, alias='customExpectations'
    )
    metrics_config: List[MetricSet] = Field(default_factory=list)

    def find_custom_expectation_group(
        self, group_name: str
    ) -> Optional[CustomExpectationGroup]:
        """
        Find a custom expectation group by name.

        Args:
            group_name (str): The name of the group to find.

        Returns:
            Optional[CustomExpectationGroup]: The found custom expectation group or None.
        """
        return next(
            (
                group
                for group in self.custom_expectations
                if group.name == group_name
            ),
            None,
        )

    def find_expectation_group(
        self, group_name: str
    ) -> Optional[ExpectationGroup]:
        """
        Find a custom expectation group by name.

        Args:
            group_name (str): The name of the group to find.

        Returns:
            Optional[CustomExpectationGroup]: The found custom expectation group or None.
        """
        return next(
            (
                group
                for group in self.great_expectations
                if group.name == group_name
            ),
            None,
        )
