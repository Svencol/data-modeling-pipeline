"""
PostgreSQL Loader Module

Handles loading DataFrames into PostgreSQL with support for
different load strategies (append, replace, upsert).
"""

from typing import Optional, Literal
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import logging
import os

logger = logging.getLogger(__name__)


class PostgresLoader:
    """Loader for PostgreSQL database targets."""
    
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None
    ):
        """
        Initialize PostgreSQL loader.
        
        Connection parameters can be provided directly or via environment variables:
        - POSTGRES_HOST
        - POSTGRES_PORT
        - POSTGRES_DB
        - POSTGRES_USER
        - POSTGRES_PASSWORD
        """
        self.host = host or os.getenv('POSTGRES_HOST', 'localhost')
        self.port = port or int(os.getenv('POSTGRES_PORT', '5432'))
        self.database = database or os.getenv('POSTGRES_DB', 'warehouse')
        self.user = user or os.getenv('POSTGRES_USER', 'pipeline')
        self.password = password or os.getenv('POSTGRES_PASSWORD', 'pipeline')
        
        self._engine: Optional[Engine] = None
    
    @property
    def connection_string(self) -> str:
        """Build PostgreSQL connection string."""
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )
    
    @property
    def engine(self) -> Engine:
        """Get or create SQLAlchemy engine."""
        if self._engine is None:
            self._engine = create_engine(
                self.connection_string,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10
            )
        return self._engine
    
    def load(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = 'raw',
        if_exists: Literal['append', 'replace', 'fail'] = 'append',
        chunk_size: int = 10000
    ) -> int:
        """
        Load DataFrame to PostgreSQL table.
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            schema: Target schema name
            if_exists: How to handle existing data ('append', 'replace', 'fail')
            chunk_size: Number of rows per insert batch
            
        Returns:
            Number of rows loaded
            
        Raises:
            LoadError: If load operation fails
        """
        if df.empty:
            logger.warning(f"Empty DataFrame, skipping load to {schema}.{table_name}")
            return 0
        
        logger.info(
            f"Loading {len(df)} rows to {schema}.{table_name} "
            f"(if_exists={if_exists})"
        )
        
        try:
            df.to_sql(
                name=table_name,
                con=self.engine,
                schema=schema,
                if_exists=if_exists,
                index=False,
                chunksize=chunk_size,
                method='multi'
            )
            
            logger.info(f"Successfully loaded {len(df)} rows")
            return len(df)
            
        except Exception as e:
            raise LoadError(f"Failed to load data: {e}")
    
    def upsert(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str,
        key_columns: list[str],
        update_columns: Optional[list[str]] = None
    ) -> int:
        """
        Upsert DataFrame to PostgreSQL using INSERT ... ON CONFLICT.
        
        Args:
            df: DataFrame to upsert
            table_name: Target table name
            schema: Target schema name
            key_columns: Columns that form the unique key
            update_columns: Columns to update on conflict (defaults to all non-key)
            
        Returns:
            Number of rows processed
        """
        if df.empty:
            return 0
        
        if update_columns is None:
            update_columns = [c for c in df.columns if c not in key_columns]
        
        # Build upsert SQL
        columns = df.columns.tolist()
        col_list = ', '.join(columns)
        placeholders = ', '.join([f':{c}' for c in columns])
        key_list = ', '.join(key_columns)
        
        update_set = ', '.join([f'{c} = EXCLUDED.{c}' for c in update_columns])
        
        sql = f"""
        INSERT INTO {schema}.{table_name} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT ({key_list})
        DO UPDATE SET {update_set}
        """
        
        logger.info(f"Upserting {len(df)} rows to {schema}.{table_name}")
        
        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                conn.execute(text(sql), row.to_dict())
        
        return len(df)
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a query and return results as DataFrame."""
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn)
    
    def test_connection(self) -> bool:
        """Test database connectivity."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection successful")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def close(self):
        """Close database connections."""
        if self._engine:
            self._engine.dispose()
            self._engine = None


class LoadError(Exception):
    """Custom exception for load failures."""
    pass
