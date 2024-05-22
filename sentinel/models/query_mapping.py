# models/query_mapping.py
"""
Module containing query mapping models.
"""
from typing import List, Optional

from pydantic import BaseModel

from .base import to_snake


class QueryStep(BaseModel):
    """
    Model representing a single step in a query mapping.
    """

    name: str
    sql: str
    create_temp_view: Optional[bool] = None
    column_mapping_name: Optional[str] = None
    persist: Optional[bool] = None

    class Config:
        alias_generator = to_snake
        populate_by_name = True


class QueryMapping(BaseModel):
    """
    Model representing a query mapping configuration.
    """

    name: Optional[str] = None
    steps: Optional[List[QueryStep]] = None

    class Config:
        alias_generator = to_snake
        populate_by_name = True
