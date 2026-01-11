"""
Schema Validator Module

Provides data validation using Pydantic models for type checking
and constraint validation before loading to the warehouse.
"""

from typing import Optional, Any
from datetime import datetime
from pydantic import BaseModel, Field, field_validator, ValidationError
import pandas as pd
import logging

logger = logging.getLogger(__name__)


# Pydantic models for data validation

class CustomerRecord(BaseModel):
    """Schema for customer records."""
    customer_id: str = Field(..., min_length=1)
    first_name: str = Field(..., min_length=1)
    last_name: str = Field(..., min_length=1)
    email: str = Field(..., pattern=r'^[\w\.-]+@[\w\.-]+\.\w+$')
    country: str = Field(..., min_length=2)
    created_at: Optional[datetime] = None
    
    @field_validator('email')
    @classmethod
    def lowercase_email(cls, v: str) -> str:
        return v.lower()


class ProductRecord(BaseModel):
    """Schema for product records."""
    product_id: str = Field(..., min_length=1)
    product_name: str = Field(..., min_length=1)
    category: str = Field(..., min_length=1)
    price: float = Field(..., gt=0)
    cost: float = Field(..., ge=0)
    
    @field_validator('price', 'cost')
    @classmethod
    def round_decimal(cls, v: float) -> float:
        return round(v, 2)


class OrderRecord(BaseModel):
    """Schema for order records."""
    order_id: str = Field(..., min_length=1)
    customer_id: str = Field(..., min_length=1)
    product_id: str = Field(..., min_length=1)
    quantity: int = Field(..., gt=0)
    order_date: Optional[datetime] = None
    status: str = Field(..., pattern=r'^(pending|completed|shipped|cancelled)$')


class SchemaValidator:
    """
    Validates DataFrames against Pydantic schemas.
    
    Supports row-level validation with configurable error handling:
    - strict: Raise on first error
    - filter: Remove invalid rows
    - flag: Add validation flag column
    """
    
    SCHEMAS = {
        'customers': CustomerRecord,
        'products': ProductRecord,
        'orders': OrderRecord
    }
    
    def __init__(self, mode: str = 'filter'):
        """
        Initialize validator.
        
        Args:
            mode: Validation mode ('strict', 'filter', 'flag')
        """
        if mode not in ('strict', 'filter', 'flag'):
            raise ValueError(f"Invalid mode: {mode}")
        self.mode = mode
    
    def validate(
        self,
        df: pd.DataFrame,
        schema_name: str
    ) -> tuple[pd.DataFrame, list[dict]]:
        """
        Validate DataFrame against schema.
        
        Args:
            df: DataFrame to validate
            schema_name: Name of schema to use
            
        Returns:
            Tuple of (validated DataFrame, list of errors)
            
        Raises:
            ValueError: In strict mode, if any row is invalid
        """
        if schema_name not in self.SCHEMAS:
            raise ValueError(f"Unknown schema: {schema_name}")
        
        schema = self.SCHEMAS[schema_name]
        valid_rows = []
        errors = []
        
        logger.info(f"Validating {len(df)} rows against {schema_name} schema")
        
        for idx, row in df.iterrows():
            try:
                # Convert row to dict, handling NaN values
                row_dict = {
                    k: (None if pd.isna(v) else v)
                    for k, v in row.to_dict().items()
                    if not k.startswith('_')  # Skip metadata columns
                }
                
                # Validate and get cleaned data
                validated = schema(**row_dict)
                valid_row = row.to_dict()
                valid_row.update(validated.model_dump())
                
                if self.mode == 'flag':
                    valid_row['_is_valid'] = True
                valid_rows.append(valid_row)
                
            except ValidationError as e:
                error_info = {
                    'row_index': idx,
                    'errors': e.errors(),
                    'data': row.to_dict()
                }
                errors.append(error_info)
                
                if self.mode == 'strict':
                    logger.error(f"Validation failed at row {idx}: {e}")
                    raise ValueError(f"Validation failed: {e}")
                elif self.mode == 'flag':
                    flagged_row = row.to_dict()
                    flagged_row['_is_valid'] = False
                    flagged_row['_validation_errors'] = str(e.errors())
                    valid_rows.append(flagged_row)
        
        if errors:
            logger.warning(
                f"Validation completed with {len(errors)} errors "
                f"({len(valid_rows)} valid rows)"
            )
        else:
            logger.info(f"All {len(valid_rows)} rows passed validation")
        
        result_df = pd.DataFrame(valid_rows)
        return result_df, errors
    
    def get_schema_columns(self, schema_name: str) -> list[str]:
        """Get list of columns defined in a schema."""
        if schema_name not in self.SCHEMAS:
            raise ValueError(f"Unknown schema: {schema_name}")
        
        schema = self.SCHEMAS[schema_name]
        return list(schema.model_fields.keys())
