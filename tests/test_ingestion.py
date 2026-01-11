"""
Unit tests for the ingestion module.

Tests cover extractors, validators, and basic functionality
without requiring a live database connection.
"""

import pytest
import pandas as pd
from pathlib import Path
from datetime import datetime
import tempfile
import os

from ingestion.extractors import BaseExtractor, ExtractionError
from ingestion.extractors.csv_extractor import CSVExtractor
from ingestion.validators import SchemaValidator


class TestCSVExtractor:
    """Tests for CSV extraction functionality."""
    
    def test_extract_valid_csv(self, tmp_path):
        """Test extracting a valid CSV file."""
        # Create test CSV
        csv_content = "id,name,value\n1,test,100\n2,test2,200"
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)
        
        extractor = CSVExtractor(file_path=csv_file)
        df = extractor.extract()
        
        assert len(df) == 2
        assert 'id' in df.columns
        assert '_loaded_at' in df.columns
        assert '_source' in df.columns
    
    def test_extract_missing_file(self, tmp_path):
        """Test that missing file raises ExtractionError."""
        extractor = CSVExtractor(file_path=tmp_path / "nonexistent.csv")
        
        with pytest.raises(ExtractionError):
            extractor.extract()
    
    def test_metadata_columns_added(self, tmp_path):
        """Test that metadata columns are properly added."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("col1\nvalue1")
        
        extractor = CSVExtractor(file_path=csv_file, source_name="test_source")
        df = extractor.extract()
        
        assert df['_source'].iloc[0] == "test_source"
        assert pd.notna(df['_loaded_at'].iloc[0])


class TestSchemaValidator:
    """Tests for schema validation functionality."""
    
    def test_valid_customer_record(self):
        """Test validation of valid customer data."""
        df = pd.DataFrame([{
            'customer_id': 'C001',
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john@example.com',
            'country': 'US',
            'created_at': datetime.now()
        }])
        
        validator = SchemaValidator(mode='strict')
        validated_df, errors = validator.validate(df, 'customers')
        
        assert len(errors) == 0
        assert len(validated_df) == 1
    
    def test_invalid_email_filtered(self):
        """Test that invalid emails are filtered in filter mode."""
        df = pd.DataFrame([
            {
                'customer_id': 'C001',
                'first_name': 'John',
                'last_name': 'Doe',
                'email': 'valid@example.com',
                'country': 'US'
            },
            {
                'customer_id': 'C002',
                'first_name': 'Jane',
                'last_name': 'Doe',
                'email': 'invalid-email',  # Invalid
                'country': 'UK'
            }
        ])
        
        validator = SchemaValidator(mode='filter')
        validated_df, errors = validator.validate(df, 'customers')
        
        assert len(errors) == 1
        assert len(validated_df) == 1
        assert validated_df['customer_id'].iloc[0] == 'C001'
    
    def test_valid_product_record(self):
        """Test validation of valid product data."""
        df = pd.DataFrame([{
            'product_id': 'P001',
            'product_name': 'Test Product',
            'category': 'Electronics',
            'price': 99.99,
            'cost': 50.00
        }])
        
        validator = SchemaValidator(mode='strict')
        validated_df, errors = validator.validate(df, 'products')
        
        assert len(errors) == 0
    
    def test_negative_price_rejected(self):
        """Test that negative prices are rejected."""
        df = pd.DataFrame([{
            'product_id': 'P001',
            'product_name': 'Test Product',
            'category': 'Electronics',
            'price': -10.00,  # Invalid
            'cost': 5.00
        }])
        
        validator = SchemaValidator(mode='filter')
        validated_df, errors = validator.validate(df, 'products')
        
        assert len(errors) == 1
    
    def test_valid_order_record(self):
        """Test validation of valid order data."""
        df = pd.DataFrame([{
            'order_id': 'O001',
            'customer_id': 'C001',
            'product_id': 'P001',
            'quantity': 2,
            'order_date': datetime.now(),
            'status': 'completed'
        }])
        
        validator = SchemaValidator(mode='strict')
        validated_df, errors = validator.validate(df, 'orders')
        
        assert len(errors) == 0
    
    def test_invalid_status_rejected(self):
        """Test that invalid order status is rejected."""
        df = pd.DataFrame([{
            'order_id': 'O001',
            'customer_id': 'C001',
            'product_id': 'P001',
            'quantity': 1,
            'order_date': datetime.now(),
            'status': 'invalid_status'  # Invalid
        }])
        
        validator = SchemaValidator(mode='filter')
        validated_df, errors = validator.validate(df, 'orders')
        
        assert len(errors) == 1


class TestBaseExtractor:
    """Tests for base extractor functionality."""
    
    def test_validate_schema_success(self):
        """Test schema validation with all required columns."""
        
        class TestExtractor(BaseExtractor):
            def extract(self):
                return pd.DataFrame()
        
        extractor = TestExtractor(source_name="test")
        df = pd.DataFrame({'col1': [1], 'col2': [2], 'col3': [3]})
        
        result = extractor.validate_schema(df, ['col1', 'col2'])
        assert result is True
    
    def test_validate_schema_missing_columns(self):
        """Test schema validation fails with missing columns."""
        
        class TestExtractor(BaseExtractor):
            def extract(self):
                return pd.DataFrame()
        
        extractor = TestExtractor(source_name="test")
        df = pd.DataFrame({'col1': [1]})
        
        with pytest.raises(ValueError) as exc_info:
            extractor.validate_schema(df, ['col1', 'col2', 'col3'])
        
        assert 'col2' in str(exc_info.value) or 'col3' in str(exc_info.value)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
