"""
Base loader service for PostgreSQL.
"""
from __future__ import annotations

import sys
from pathlib import Path
from abc import ABC

# Add Data root to path
# Data/staging/common/services/base_loader.py -> parents[3] should be Data/
DATA_ROOT = Path(__file__).resolve().parents[3]
if str(DATA_ROOT) not in sys.path:
    sys.path.append(str(DATA_ROOT))

# Adjust import based on where database folder is. 
# It is at Data/database.
from staging.common.services.connection import get_staging_db

class BaseLoader(ABC):
    """
    Base class for bulk loaders.
    """

    def __init__(self, batch_id: str):
        """
        Initialize the loader.

        Args:
            batch_id: Batch identifier for this ingestion run
        """
        self.batch_id = batch_id
        self.db = get_staging_db()
        self.stats = {
            'inserted': 0,
            'updated': 0,
            'skipped': 0,
        }

    def get_stats(self) -> dict:
        """Get cumulative statistics for this loader instance."""
        return self.stats
