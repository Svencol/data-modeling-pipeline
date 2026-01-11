"""
Base Extractor Module

Defines the abstract interface for all data extractors.
New data sources should inherit from BaseExtractor and implement the extract method.
"""

from abc import ABC, abstractmethod
from typing import Optional
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """Abstract base class for data extractors."""
    
    def __init__(self, source_name: str):
        """
        Initialize the extractor.
        
        Args:
            source_name: Identifier for the data source (used in metadata)
        """
        self.source_name = source_name
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """
        Extract data from the source.
        
        Returns:
            DataFrame containing the extracted data
            
        Raises:
            ExtractionError: If extraction fails
        """
        pass
    
    def add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add ingestion metadata columns to the DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with added metadata columns
        """
        df = df.copy()
        df['_loaded_at'] = pd.Timestamp.now()
        df['_source'] = self.source_name
        return df
    
    def validate_schema(
        self, 
        df: pd.DataFrame, 
        required_columns: list[str]
    ) -> bool:
        """
        Validate that the DataFrame contains required columns.
        
        Args:
            df: DataFrame to validate
            required_columns: List of column names that must be present
            
        Returns:
            True if validation passes
            
        Raises:
            ValueError: If required columns are missing
        """
        missing = set(required_columns) - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        return True


class ExtractionError(Exception):
    """Custom exception for extraction failures."""
    pass
