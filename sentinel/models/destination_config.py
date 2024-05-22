# models/destination_config.py
"""
Module containing destination configuration model.
"""
from typing import Dict, Optional

from pydantic import Field

from sentinel.utils.string_utils import to_snake

from .base import BaseConfig


class DestinationConfig(BaseConfig):
    """
    Model representing destination configuration settings.
    """

    source_type: str = Field(..., alias='sourceType')
    format: str = Field(...)
    merge_statement: Optional[Dict[str, str]] = Field(
        None, alias='mergeStatement'
    )
    yaml_path: Optional[str] = Field(None, alias='yamlPath')

    class Config:
        alias_generator = to_snake
        populate_by_name = True
