# models/source_config.py
"""
Module containing source configuration model.
"""
from typing import Dict, List, Optional

from pydantic import Field

from sentinel.utils.string_utils import to_snake

from .base import BaseConfig, CustomSchema


class SourceConfig(BaseConfig):
    """
    Model representing source configuration settings.
    """

    custom_schemas: Optional[List[CustomSchema]] = Field(
        None, alias='customSchemas'
    )
    mask_fields: Optional[List[str]] = Field(None, alias='maskFields')
    card_numbers_columns: Optional[List[str]] = Field(
        None, alias='cardNumbersColumns'
    )
    deduplicate_sort_columns: Optional[List[str]] = Field(
        None, alias='deduplicateSortColumns'
    )
    zorder_columns: Optional[List[str]] = Field(None, alias='zorderColumns')
    pipeline: Optional[str] = None
    source_type: str = Field(..., alias='sourceType')
    format: str = Field(...)
    col_name_file_path: Optional[str] = Field(None, alias='colNameFilePath')
    landing_options: Optional[Dict[str, str]] = Field(
        None, alias='landingOptions'
    )

    class Config:
        alias_generator = to_snake
        populate_by_name = True
