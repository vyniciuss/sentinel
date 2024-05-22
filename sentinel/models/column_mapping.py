# models/column_mapping.py
"""
Module containing column mapping models.
"""
from typing import List, Optional

from pydantic import BaseModel


class Column(BaseModel):
    """
    Model representing a column mapping.
    """

    fieldName: str
    aliasName: Optional[str]
    dataType: str
    comment: Optional[str] = ''
    customTransformation: Optional[str] = None
    isDerivedColumn: Optional[bool] = None


class ColumnMapping(BaseModel):
    """
    Model representing a mapping of multiple columns.
    """

    name: str
    columns: List[Column]
    identifier: Optional[str] = None
