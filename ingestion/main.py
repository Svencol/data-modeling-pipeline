"""
Main Ingestion Module

Entry point for the data ingestion pipeline. Orchestrates extraction,
validation, and loading for all configured data sources.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime

from ingestion.extractors.csv_extractor import CSVExtractor
from ingestion.loaders import PostgresLoader, LoadError
from ingestion.validators import SchemaValidator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class IngestionPipeline:
    """
    Orchestrates the data ingestion pipeline.
    
    Handles extraction from configured sources, validation against
    schemas, and loading to the data warehouse.
    """
    
    def __init__(self, data_dir: str = '/app/data'):
        """
        Initialize ingestion pipeline.
        
        Args:
            data_dir: Directory containing source data files
        """
        self.data_dir = Path(data_dir)
        self.loader = PostgresLoader()
        self.validator = SchemaValidator(mode='filter')
        
        # Track ingestion metrics
        self.metrics = {
            'start_time': None,
            'end_time': None,
            'tables_processed': 0,
            'rows_loaded': 0,
            'rows_rejected': 0,
            'errors': []
        }
    
    def run(self) -> dict:
        """
        Execute the full ingestion pipeline.
        
        Returns:
            Dictionary containing ingestion metrics
        """
        self.metrics['start_time'] = datetime.now()
        logger.info("Starting ingestion pipeline")
        
        # Test database connection
        if not self.loader.test_connection():
            raise RuntimeError("Cannot connect to database")
        
        # Define ingestion jobs
        jobs = [
            {
                'name': 'customers',
                'file': 'customers.csv',
                'table': 'customers',
                'schema': 'raw',
                'date_columns': ['created_at']
            },
            {
                'name': 'products',
                'file': 'products.csv',
                'table': 'products',
                'schema': 'raw',
                'date_columns': []
            },
            {
                'name': 'orders',
                'file': 'orders.csv',
                'table': 'orders',
                'schema': 'raw',
                'date_columns': ['order_date']
            }
        ]
        
        for job in jobs:
            try:
                self._process_job(job)
                self.metrics['tables_processed'] += 1
            except Exception as e:
                logger.error(f"Failed to process {job['name']}: {e}")
                self.metrics['errors'].append({
                    'job': job['name'],
                    'error': str(e)
                })
        
        self.metrics['end_time'] = datetime.now()
        self._log_summary()
        
        return self.metrics
    
    def _process_job(self, job: dict) -> None:
        """
        Process a single ingestion job.
        
        Args:
            job: Job configuration dictionary
        """
        logger.info(f"Processing {job['name']}")
        
        file_path = self.data_dir / job['file']
        
        # Extract
        extractor = CSVExtractor(
            file_path=file_path,
            source_name=f"csv_{job['name']}",
            parse_dates=job.get('date_columns', [])
        )
        df = extractor.extract()
        
        # Validate
        df_valid, errors = self.validator.validate(df, job['name'])
        self.metrics['rows_rejected'] += len(errors)
        
        if errors:
            logger.warning(f"Rejected {len(errors)} invalid rows from {job['name']}")
        
        # Load
        rows_loaded = self.loader.load(
            df=df_valid,
            table_name=job['table'],
            schema=job['schema'],
            if_exists='append'
        )
        self.metrics['rows_loaded'] += rows_loaded
    
    def _log_summary(self) -> None:
        """Log ingestion summary."""
        duration = (
            self.metrics['end_time'] - self.metrics['start_time']
        ).total_seconds()
        
        logger.info("=" * 50)
        logger.info("INGESTION SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Tables processed: {self.metrics['tables_processed']}")
        logger.info(f"Rows loaded: {self.metrics['rows_loaded']}")
        logger.info(f"Rows rejected: {self.metrics['rows_rejected']}")
        
        if self.metrics['errors']:
            logger.warning(f"Errors: {len(self.metrics['errors'])}")
            for err in self.metrics['errors']:
                logger.warning(f"  - {err['job']}: {err['error']}")


def main():
    """Main entry point."""
    try:
        pipeline = IngestionPipeline()
        metrics = pipeline.run()
        
        if metrics['errors']:
            sys.exit(1)
        sys.exit(0)
        
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
