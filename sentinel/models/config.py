# models/config.py
"""
Module containing the main configuration model.
"""
from typing import List, Optional

from pydantic import BaseModel, Field

from sentinel.utils.string_utils import to_snake

from .base import SparkConfigurations
from .column_mapping import ColumnMapping
from .destination_config import DestinationConfig
from .query_mapping import QueryMapping
from .source_config import SourceConfig


class Config(BaseModel):
    """
    Model representing the overall configuration.
    """

    flow_type: Optional[str] = Field(None, alias='flowType')
    spark_configurations: Optional[SparkConfigurations] = Field(
        None, alias='sparkConfigurations'
    )
    sources_config: Optional[List[SourceConfig]] = Field(
        None, alias='sourcesConfig'
    )
    destinations_config: Optional[List[DestinationConfig]] = Field(
        None, alias='destinationsConfig'
    )
    column_mappings: Optional[List[ColumnMapping]] = Field(
        None, alias='columnMappings'
    )
    query_mappings: Optional[List[QueryMapping]] = Field(
        None, alias='queryMappings'
    )

    class Config:
        alias_generator = to_snake
        populate_by_name = True

    def find_column_mapping(self, name: str) -> Optional[ColumnMapping]:
        """
        Find a column mapping by name.

        Args:
            name (str): The name of the column mapping.

        Returns:
            Optional[ColumnMapping]: The found column mapping or None.
        """
        return next(
            (
                mapping
                for mapping in (self.column_mappings or [])
                if mapping.name == name
            ),
            None,
        )

    def find_query_mapping(self, name: str) -> Optional[QueryMapping]:
        """
        Find a query mapping by name.

        Args:
            name (str): The name of the query mapping.

        Returns:
            Optional[QueryMapping]: The found query mapping or None.
        """
        return next(
            (
                mapping
                for mapping in (self.query_mappings or [])
                if mapping.name == name
            ),
            None,
        )

    def find_source_config(self, name: str) -> Optional[SourceConfig]:
        """
        Find a source config by name.

        Args:
            name (str): The name of the source config.

        Returns:
            Optional[SourceConfig]: The found source config or None.
        """
        return next(
            (
                config
                for config in (self.sources_config or [])
                if config.name == name
            ),
            None,
        )

    def find_destination_config(
        self, name: str
    ) -> Optional[DestinationConfig]:
        """
        Find a destination config by name.

        Args:
            name (str): The name of the destination config.

        Returns:
            Optional[DestinationConfig]: The found destination config or None.
        """
        return next(
            (
                config
                for config in (self.destinations_config or [])
                if config.name == name
            ),
            None,
        )
