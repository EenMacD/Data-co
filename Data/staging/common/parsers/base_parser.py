"""
Base parser for Companies House bulk data products.
Provides common ZIP/CSV handling with chunked reading for memory efficiency.
"""
from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterator, Any
from zipfile import ZipFile

import pandas as pd


class BulkDataParser(ABC):
    """
    Abstract base class for parsing Companies House bulk data files.

    Subclasses should define:
    - FIELD_MAPPINGS: dict mapping source column names to target column names
    - TARGET_TABLE: the staging table this data goes into
    - HASH_FIELDS: list of fields to include in change detection hash
    """

    FIELD_MAPPINGS: dict[str, str] = {}
    TARGET_TABLE: str = 'staging_companies'
    HASH_FIELDS: list[str] = []
    CHUNK_SIZE: int = 100_000  # Rows per chunk

    def __init__(self, file_path: Path):
        """
        Initialize parser with path to ZIP file.

        Args:
            file_path: Path to the ZIP file to parse
        """
        self.file_path = Path(file_path)
        self._total_rows: int | None = None

    @property
    def total_rows(self) -> int | None:
        """Return total row count if known."""
        return self._total_rows

    def parse_chunks(self) -> Iterator[pd.DataFrame]:
        """
        Parse the ZIP file and yield DataFrames in chunks.

        Yields:
            pd.DataFrame: Normalized chunks of data ready for insertion
        """
        with ZipFile(self.file_path, 'r') as archive:
            # Find the CSV file in the archive
            csv_filename = self._find_csv_in_archive(archive)
            if not csv_filename:
                raise ValueError(f"No CSV file found in {self.file_path}")

            with archive.open(csv_filename) as csv_file:
                # Use chunked reading for memory efficiency
                chunk_iter = pd.read_csv(
                    csv_file,
                    chunksize=self.CHUNK_SIZE,
                    dtype=str,  # Read all as strings initially
                    low_memory=False,
                    on_bad_lines='warn'
                )

                for chunk in chunk_iter:
                    # Clean column names
                    chunk.columns = chunk.columns.str.strip()

                    # Apply field mappings
                    normalized = self.normalize_columns(chunk)

                    # Apply any custom transformations
                    transformed = self.transform(normalized)

                    # Compute data hash for change detection
                    if self.HASH_FIELDS:
                        transformed['data_hash'] = transformed.apply(
                            self._compute_row_hash, axis=1
                        )

                    yield transformed

    def _find_csv_in_archive(self, archive: ZipFile) -> str | None:
        """Find the first CSV file in the ZIP archive."""
        for name in archive.namelist():
            if name.lower().endswith('.csv'):
                return name
        return None

    def normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply field mappings to standardize column names.

        Args:
            df: DataFrame with original column names

        Returns:
            DataFrame with renamed columns (only mapped columns are kept)
        """
        # Find which source columns exist in this DataFrame
        available_mappings = {
            src: dst for src, dst in self.FIELD_MAPPINGS.items()
            if src in df.columns
        }

        # Select and rename only the mapped columns
        result = df[list(available_mappings.keys())].rename(columns=available_mappings)

        return result

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply any custom transformations to the data.
        Override in subclasses for product-specific transformations.

        Args:
            df: DataFrame with normalized column names

        Returns:
            Transformed DataFrame
        """
        return df

    def _compute_row_hash(self, row: pd.Series) -> str:
        """
        Compute MD5 hash of specified fields for change detection.

        Args:
            row: A single row from the DataFrame

        Returns:
            MD5 hash string (32 characters)
        """
        values = []
        for field in self.HASH_FIELDS:
            val = row.get(field, '')
            # Handle list/dict types first (since they cause pd.isna to fail)
            if isinstance(val, (list, dict)):
                val = str(val)
            # Handle NaN and None
            elif pd.isna(val):
                val = ''
            values.append(str(val))

        hash_string = '|'.join(values)
        return hashlib.md5(hash_string.encode('utf-8')).hexdigest()

    @classmethod
    def compute_hash(cls, data: dict[str, Any]) -> str:
        """
        Compute MD5 hash from a dictionary of field values.

        Args:
            data: Dictionary with field names as keys

        Returns:
            MD5 hash string (32 characters)
        """
        values = []
        for field in cls.HASH_FIELDS:
            val = data.get(field, '')
            if isinstance(val, (list, dict)):
                val = str(val)
            elif val is None or (isinstance(val, float) and pd.isna(val)):
                val = ''
            values.append(str(val))

        hash_string = '|'.join(values)
        return hashlib.md5(hash_string.encode('utf-8')).hexdigest()
