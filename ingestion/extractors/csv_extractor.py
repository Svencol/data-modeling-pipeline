"""
CSV Extractor Module

Extracts data from CSV files with configurable options for
delimiter, encoding, and data type inference.
"""

from pathlib import Path
from typing import Optional
import pandas as pd

from . import BaseExtractor, ExtractionError


class CSVExtractor(BaseExtractor):
    """Extractor for CSV file sources."""
    
    def __init__(
        self,
        file_path: str | Path,
        source_name: Optional[str] = None,
        delimiter: str = ',',
        encoding: str = 'utf-8',
        parse_dates: Optional[list[str]] = None
    ):
        """
        Initialize CSV extractor.
        
        Args:
            file_path: Path to the CSV file
            source_name: Optional name for the source (defaults to filename)
            delimiter: Column delimiter character
            encoding: File encoding
            parse_dates: List of column names to parse as dates
        """
        self.file_path = Path(file_path)
        self.delimiter = delimiter
        self.encoding = encoding
        self.parse_dates = parse_dates or []
        
        source = source_name or self.file_path.stem
        super().__init__(source_name=source)
    
    def extract(self) -> pd.DataFrame:
        """
        Extract data from CSV file.
        
        Returns:
            DataFrame containing the CSV data
            
        Raises:
            ExtractionError: If file cannot be read
        """
        self.logger.info(f"Extracting data from {self.file_path}")
        
        if not self.file_path.exists():
            raise ExtractionError(f"File not found: {self.file_path}")
        
        try:
            df = pd.read_csv(
                self.file_path,
                delimiter=self.delimiter,
                encoding=self.encoding,
                parse_dates=self.parse_dates if self.parse_dates else False
            )
            
            self.logger.info(f"Extracted {len(df)} rows from {self.file_path.name}")
            return self.add_metadata(df)
            
        except pd.errors.ParserError as e:
            raise ExtractionError(f"Failed to parse CSV: {e}")
        except Exception as e:
            raise ExtractionError(f"Extraction failed: {e}")
