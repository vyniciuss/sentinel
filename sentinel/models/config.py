# models/config.py
"""
Module containing the main configuration model.
"""
from typing import List, Optional

from pydantic import BaseModel, Field

from .base import DestinationConfig, SourceConfig
from .expectations import DataQualityConfig


class Config(BaseModel):
    """
    Model representing the overall configuration.
    """

    sources_config: Optional[List[SourceConfig]] = Field(
        None, alias='sourcesConfig'
    )
    destinations_config: Optional[List[DestinationConfig]] = Field(
        None, alias='destinationsConfig'
    )
    data_quality: Optional[DataQualityConfig] = Field(
        None, alias='dataQuality'
    )

    class Config:
        populate_by_name = True

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
