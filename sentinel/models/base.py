# models/base.py
"""Module containing base model definitions.
"""
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from sentinel.utils.string_utils import to_snake


class UDF(BaseModel):
    """
    Model representing a User Defined Function (UDF).
    """

    class_name: Optional[str] = Field(None, alias='className')
    method_name: Optional[str] = Field(None, alias='methodName')

    class Config:
        alias_generator = to_snake
        populate_by_name = True


class SparkConfigurations(BaseModel):
    """
    Model representing Spark configuration settings.
    """

    udfs: Optional[List[UDF]] = None
    spark_options: Optional[Dict[str, str]] = Field(None, alias='sparkOptions')

    class Config:
        alias_generator = to_snake
        populate_by_name = True


class CustomSchema(BaseModel):
    """
    Model representing custom schema settings.
    """

    name: Optional[str] = None
    schema_type: Optional[str] = Field(None, alias='schemaType')
    purpose: Optional[str] = None
    schema_location: Optional[str] = Field(None, alias='schemaLocation')

    class Config:
        alias_generator = to_snake
        populate_by_name = True


class BaseConfig(BaseModel):
    """
    Base model for common configuration settings.
    """

    name: Optional[str] = None
    table_name: Optional[str] = Field(None, alias='tableName')
    table_comment: Optional[str] = Field(None, alias='tableComment')
    database: Optional[str] = None
    catalog: Optional[str] = None
    create_table: Optional[bool] = Field(None, alias='createTable')
    path_database: Optional[str] = Field(None, alias='pathDatabase')
    use_tenant_and_country: Optional[bool] = Field(
        None, alias='useTenantAndCountry'
    )
    keys: Optional[List[str]] = None
    partitioning_keys: Optional[List[str]] = Field(
        None, alias='partitioningKeys'
    )
    raw_options: Optional[Dict[str, str]] = Field(None, alias='rawOptions')
    stage_options: Optional[Dict[str, str]] = Field(None, alias='stageOptions')
    common_options: Optional[Dict[str, str]] = Field(
        None, alias='commonOptions'
    )
    refined_options: Optional[Dict[str, str]] = Field(
        None, alias='refinedOptions'
    )
    optional_field: Optional[str] = None

    class Config:
        alias_generator = to_snake
        populate_by_name = True
