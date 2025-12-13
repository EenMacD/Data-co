"""
Parser for Companies House PSC (Persons with Significant Control) bulk JSON files.

Note: PSC bulk data is in JSON format, not CSV.
Each record represents a PSC entry for a company.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Iterator, Any
from zipfile import ZipFile

import pandas as pd


class PSCDataParser:
    """
    Parser for PSC snapshot JSON files from Companies House.

    Source format: psc-snapshot-YYYY-MM-DD_XofY.zip
    Contains JSON records with PSC data.

    The JSON structure is a series of line-delimited JSON objects,
    each containing company_number and PSC details.
    """

    TARGET_TABLE = 'staging_officers'
    CHUNK_SIZE = 50_000  # JSON records per chunk

    # Fields used to compute the change detection hash
    HASH_FIELDS = [
        'company_number',
        'officer_name',
        'officer_role',
        'date_of_birth',
        'nationality',
        'occupation',
        'resigned_on',
        'locality',
        'postal_code',
        'address_line_1',
        'address_line_2',
        'country',
    ]

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

        PSC data is line-delimited JSON (JSONL format).

        Yields:
            pd.DataFrame: Normalized chunks of data ready for insertion
        """
        with ZipFile(self.file_path, 'r') as archive:
            # Find the JSON file in the archive
            json_filename = self._find_json_in_archive(archive)
            if not json_filename:
                raise ValueError(f"No JSON file found in {self.file_path}")

            with archive.open(json_filename) as json_file:
                records = []
                for line in json_file:
                    try:
                        record = json.loads(line.decode('utf-8'))
                        parsed = self._parse_psc_record(record)
                        if parsed:
                            records.append(parsed)

                        # Yield chunk when we hit the size limit
                        if len(records) >= self.CHUNK_SIZE:
                            df = pd.DataFrame(records)
                            df['data_hash'] = df.apply(self._compute_row_hash, axis=1)
                            yield df
                            records = []

                    except json.JSONDecodeError:
                        continue  # Skip malformed lines

                # Yield remaining records
                if records:
                    df = pd.DataFrame(records)
                    df['data_hash'] = df.apply(self._compute_row_hash, axis=1)
                    yield df

    def _find_json_in_archive(self, archive: ZipFile) -> str | None:
        """Find the first JSON file in the ZIP archive."""
        for name in archive.namelist():
            if name.lower().endswith('.json') or name.lower().endswith('.txt'):
                return name
        # If no explicit JSON extension, return first file
        names = archive.namelist()
        return names[0] if names else None

    def _parse_psc_record(self, record: dict) -> dict | None:
        """
        Parse a single PSC JSON record into staging_officers format.

        Args:
            record: Raw JSON record from PSC bulk data

        Returns:
            Dict formatted for staging_officers table, or None if invalid
        """
        company_number = record.get('company_number')
        if not company_number:
            return None

        data = record.get('data', {})

        # Extract name - PSC records can have different name formats
        name = data.get('name')
        if not name:
            # Try individual name components
            name_parts = []
            if data.get('name_elements'):
                name_elem = data['name_elements']
                if name_elem.get('title'):
                    name_parts.append(name_elem['title'])
                if name_elem.get('forename'):
                    name_parts.append(name_elem['forename'])
                if name_elem.get('middle_name'):
                    name_parts.append(name_elem['middle_name'])
                if name_elem.get('surname'):
                    name_parts.append(name_elem['surname'])
                name = ' '.join(name_parts) if name_parts else None

        # Extract address
        address = data.get('address', {})

        # Extract date of birth
        dob = data.get('date_of_birth')
        dob_str = None
        if dob and isinstance(dob, dict):
            year = dob.get('year')
            month = dob.get('month')
            day = dob.get('day', 1) # Default to 1st if not present
            if year and month:
                dob_str = f"{year:04d}-{month:02d}-{day:02d}"

        # Determine PSC type/role
        kind = data.get('kind', 'person-with-significant-control')

        return {
            'company_number': company_number,
            'officer_name': name,
            'officer_role': kind,
            'date_of_birth': dob_str,
            'appointed_on': data.get('notified_on'),
            'resigned_on': data.get('ceased_on'),
            'nationality': data.get('nationality'),
            'occupation': None,  # PSC data doesn't include occupation
            'address_line_1': address.get('address_line_1'),
            'address_line_2': address.get('address_line_2'),
            'locality': address.get('locality'),
            'postal_code': address.get('postal_code'),
            'country': address.get('country'),
            'raw_data': record,  # Store full record for reference
        }

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
            if pd.isna(val):
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
            if val is None or (isinstance(val, float) and pd.isna(val)):
                val = ''
            values.append(str(val))

        hash_string = '|'.join(values)
        return hashlib.md5(hash_string.encode('utf-8')).hexdigest()
