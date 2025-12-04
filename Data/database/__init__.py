"""
Companies House Database Package

This package provides database connectivity and utilities for the
Companies House data platform's two-database architecture.

Usage:
    from database.connection import get_staging_db, get_production_db
    from database.inserters import StagingInserter
    from database.validators import DataValidator

Example:
    # Insert data into staging
    inserter = StagingInserter("my_search")
    company_id = inserter.insert_company(company_data)
    inserter.complete_batch()

    # Validate data quality
    validator = DataValidator(batch_id)
    results = validator.validate_batch()

    # Merge to production
    from database.merge_to_production import ProductionMerger
    merger = ProductionMerger(batch_id)
    merger.merge_batch()
"""

__version__ = "1.0.0"
__all__ = [
    "get_staging_db",
    "get_production_db",
    "StagingInserter",
    "DataValidator",
    "DataTransformer",
    "ProductionMerger",
]

from database.connection import get_staging_db, get_production_db
from database.inserters import StagingInserter
from database.validators import DataValidator, DataTransformer
from database.merge_to_production import ProductionMerger
