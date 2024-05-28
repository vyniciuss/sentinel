# models/base.py
"""Module containing base model definitions.
"""
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class BaseConfig(BaseModel):
    """
    Base model for common configuration settings.
    """

    name: Optional[str] = None
    table_name: Optional[str] = Field(None, alias='tableName')
    table_comment: Optional[str] = Field(None, alias='tableComment')
    database: Optional[str] = None
    catalog: Optional[str] = None
    keys: Optional[List[str]] = None
    partitioning_keys: Optional[List[str]] = Field(
        None, alias='partitioningKeys'
    )
    format: str = Field(...)

    class Config:
        populate_by_name = True


class SourceConfig(BaseConfig):
    """
    Model representing source configuration settings.
    """

    read_options: Optional[Dict[str, str]] = Field(None, alias='readOptions')

    class Config:
        populate_by_name = True


class DestinationConfig(BaseConfig):
    """
    Model representing destination configuration settings.
    """

    write_options: Optional[Dict[str, str]] = Field(None, alias='writeOptions')

    class Config:
        populate_by_name = True
