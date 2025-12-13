"""
Parser for Companies House Accounts bulk data files (XBRL/iXBRL format).

The Accounts bulk data contains XBRL and iXBRL formatted financial filings.
Each ZIP contains multiple account files organized by company.
"""
from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Iterator, Any, Optional, Callable
from zipfile import ZipFile
from xml.etree import ElementTree as ET

import pandas as pd


# XBRL tag mappings to standardized field names
TAG_SYNONYMS = {
    'turnover': (
        'Turnover',
        'Revenue',
        'RevenueFromContractsWithCustomersExcludingExciseDuties',
    ),
    'profit_loss': (
        'ProfitLoss',
        'ProfitLossAccount',
        'ProfitLossBeforeTax',
        'ProfitLossFromOperatingActivities',
        'NetIncomeLoss',
    ),
    'operating_profit': (
        'OperatingProfitLoss',
        'ProfitLossFromOperatingActivities',
    ),
    'cash': (
        'CashBankInHand',
        'CashAndCashEquivalents',
        'CashAndCashEquivalentsAtCarryingValue',
    ),
    'total_assets': (
        'TotalAssets',
        'Assets',
        'AssetsTotal',
    ),
    'net_assets': (
        'NetAssets',
        'NetAssetsLiabilities',
        'NetAssetsLiabilitiesIncludingNoncontrollingInterests',
    ),
    'total_liabilities': (
        'Liabilities',
        'LiabilitiesTotal',
    ),
}

# Regex patterns for iXBRL parsing (flexible namespace prefixes and attribute order)
# Pattern 1: name before contextRef
IX_NONFRACTION_RE_1 = re.compile(
    r"<(?:\w+:)?nonFraction[^>]*?name=[\"'](?P<name>[^\"']+)[\"'][^>]*?contextRef=[\"'](?P<context>[^\"']+)[\"'][^>]*?>(?P<value>.*?)</(?:\w+:)?nonFraction>",
    re.IGNORECASE | re.DOTALL,
)
# Pattern 2: contextRef before name
IX_NONFRACTION_RE_2 = re.compile(
    r"<(?:\w+:)?nonFraction[^>]*?contextRef=[\"'](?P<context>[^\"']+)[\"'][^>]*?name=[\"'](?P<name>[^\"']+)[\"'][^>]*?>(?P<value>.*?)</(?:\w+:)?nonFraction>",
    re.IGNORECASE | re.DOTALL,
)
# Pattern 3: Also try nonNumeric for text values
IX_NONNUMERIC_RE = re.compile(
    r"<(?:\w+:)?nonNumeric[^>]*?name=[\"'](?P<name>[^\"']+)[\"'][^>]*?contextRef=[\"'](?P<context>[^\"']+)[\"'][^>]*?>(?P<value>.*?)</(?:\w+:)?nonNumeric>",
    re.IGNORECASE | re.DOTALL,
)
IX_CONTEXT_RE = re.compile(
    r"<(?:\w+:)?context[^>]*?id=[\"'](?P<id>[^\"']+)[\"'][^>]*?>.*?(?:<(?:\w+:)?(?:endDate|instant)>(?P<end>[^<]+)</(?:\w+:)?(?:endDate|instant)>).*?</(?:\w+:)?context>",
    re.IGNORECASE | re.DOTALL,
)


class AccountsDataParser:
    """
    Parser for Accounts bulk data files from Companies House.

    Source format: Accounts_Bulk_Data-YYYY-MM-DD.zip
    Contains XBRL/iXBRL files with financial data.

    The accounts bulk data is structured differently - it contains
    nested ZIP files or directories with individual company accounts.
    """

    TARGET_TABLE = 'staging_financials'
    CHUNK_SIZE = 10_000  # Financial records per chunk

    # Fields used to compute the change detection hash
    HASH_FIELDS = [
        'company_number',
        'period_end',
        'turnover',
        'profit_loss',
        'total_assets',
        'total_liabilities',
        'net_worth',
    ]

    def __init__(self, file_path: Path, log_callback: Optional[Callable[[str], None]] = None):
        """
        Initialize parser with path to ZIP file.

        Args:
            file_path: Path to the ZIP file to parse
            log_callback: Optional function to log debug messages
        """
        self.file_path = Path(file_path)
        self.log_callback = log_callback or (lambda msg: None)
        self._total_rows: int | None = None

    @property
    def total_rows(self) -> int | None:
        """Return total row count if known."""
        return self._total_rows

    def parse_chunks(self) -> Iterator[pd.DataFrame]:
        """
        Parse the ZIP file and yield DataFrames in chunks.

        Yields:
            pd.DataFrame: Normalized chunks of financial data
        """
        with ZipFile(self.file_path, 'r') as archive:
            records = []
            file_list = archive.namelist()
            self.log_callback(f"Found {len(file_list)} files in archive {self.file_path.name}")
            
            # Log first 5 files to check naming convention
            for name in file_list[:5]:
                self.log_callback(f"Sample file: {name}")

            for name in archive.namelist():
                lower_name = name.lower()

                # Skip directories
                if name.endswith('/'):
                    continue

                # Process XBRL or HTML/iXBRL files
                if lower_name.endswith('.xml') or lower_name.endswith('.xbrl'):
                    data = archive.read(name)
                    parsed = self._parse_xbrl_bytes(data, name)
                    if not parsed:
                         self.log_callback(f"Parsed 0 records from XBRL file: {name}")
                    records.extend(parsed)

                elif lower_name.endswith('.html') or lower_name.endswith('.htm'):
                    data = archive.read(name)
                    parsed = self._parse_ixbrl_bytes(data, name)
                    if not parsed:
                         self.log_callback(f"Parsed 0 records from iXBRL file: {name}")
                    records.extend(parsed)

                # Check for nested ZIPs
                elif lower_name.endswith('.zip'):
                    nested_records = self._parse_nested_zip(archive, name)
                    records.extend(nested_records)

                # Yield chunk when we hit the size limit
                if len(records) >= self.CHUNK_SIZE:
                    self.log_callback(f"Yielding chunk of {len(records)} records")
                    df = pd.DataFrame(records)
                    df['data_hash'] = df.apply(self._compute_row_hash, axis=1)
                    yield df
                    records = []

            # Yield remaining records
            if records:
                df = pd.DataFrame(records)
                df['data_hash'] = df.apply(self._compute_row_hash, axis=1)
                yield df

    def _parse_nested_zip(self, parent_archive: ZipFile, nested_name: str) -> list[dict]:
        """Parse a ZIP file nested within the main archive."""
        records = []
        try:
            nested_data = parent_archive.read(nested_name)
            import io
            with ZipFile(io.BytesIO(nested_data), 'r') as nested_archive:
                for name in nested_archive.namelist():
                    lower_name = name.lower()
                    if name.endswith('/'):
                        continue

                    if lower_name.endswith('.xml') or lower_name.endswith('.xbrl'):
                        data = nested_archive.read(name)
                        records.extend(self._parse_xbrl_bytes(data, name))
                    elif lower_name.endswith('.html') or lower_name.endswith('.htm'):
                        data = nested_archive.read(name)
                        records.extend(self._parse_ixbrl_bytes(data, name))
        except Exception as e:
            print(f"Warning: Failed to parse nested ZIP {nested_name}: {e}")
        return records

    def _extract_company_number(self, filename: str) -> str | None:
        """
        Extract company number from filename.

        Accounts files are typically named like:
        - Prod223_0123_00012345_20230101.xml
        - 00012345_accounts.html
        """
        # Try to find an 8-digit company number
        match = re.search(r'\b(\d{8})\b', filename)
        if match:
            return match.group(1)
        # Try alphanumeric format (SC123456, etc.)
        match = re.search(r'\b([A-Z]{2}\d{6})\b', filename)
        if match:
            return match.group(1)
        return None

    def _extract_company_number_from_xbrl(self, root: ET.Element) -> str | None:
        """
        Extract company number from XBRL XML content.

        Looks for the identifier element within entity/context structures.
        """
        # Look for identifier elements - these contain the company number
        # Typical structure: <entity><identifier scheme="...">12345678</identifier></entity>
        for identifier in root.findall('.//*{*}identifier'):
            text = (identifier.text or '').strip()
            if text:
                # Clean up the company number (remove leading zeros if 8 digits)
                if len(text) == 8 and text.isdigit():
                    return text
                # Handle formats like SC123456
                if re.match(r'^[A-Z]{2}\d{6}$', text):
                    return text
        return None

    def _extract_company_number_from_ixbrl(self, html_text: str) -> str | None:
        """
        Extract company number from iXBRL HTML content.

        Looks for the identifier element within entity/context structures.
        """
        # Search for identifier elements in HTML
        identifier_pattern = re.compile(
            r'<(?:xbrli:)?identifier[^>]*>([^<]+)</(?:xbrli:)?identifier>',
            re.IGNORECASE
        )
        match = identifier_pattern.search(html_text)
        if match:
            company_number = match.group(1).strip()
            # Validate it looks like a company number
            if len(company_number) == 8 and company_number.isdigit():
                return company_number
            if re.match(r'^[A-Z]{2}\d{6}$', company_number):
                return company_number
        return None

    def _parse_xbrl_bytes(self, data: bytes, filename: str) -> list[dict]:
        """
        Parse XBRL XML data from bytes.

        Returns list of financial records.
        """
        # Try to extract company number from filename first
        company_number = self._extract_company_number(filename)

        records = []
        try:
            root = ET.fromstring(data)
        except Exception as e:
            print(f"Warning: XBRL parse failed for {filename}: {e}")
            return []

        # If filename didn't have company number, try to extract from XBRL content
        if not company_number:
            company_number = self._extract_company_number_from_xbrl(root)
            if company_number:
                self.log_callback(f"Extracted company number {company_number} from XBRL content in {filename}")

        if not company_number:
            self.log_callback(f"Skipping {filename}: No company number found in filename or XBRL content")
            return []

        # Build context id -> period_end mapping
        ctx_end: dict[str, str] = {}
        for ctx in root.findall('.//*{*}context'):
            ctx_id = ctx.attrib.get('id')
            if not ctx_id:
                continue
            end_el = ctx.find('.//*{*}endDate')
            inst_el = ctx.find('.//*{*}instant')
            end_val = (end_el.text if end_el is not None else None) or \
                      (inst_el.text if inst_el is not None else None)
            if end_val:
                ctx_end[ctx_id] = end_val

        # Collect facts by period
        facts_by_period: dict[str, dict[str, float]] = {}

        # Debug: Log first few tags to see what we're working with
        tag_sample = []
        for idx, el in enumerate(root.iter()):
            if idx < 20:  # Sample first 20 tags
                tag_sample.append(self._localname(el.tag))

        for el in root.iter():
            tag = self._localname(el.tag)
            if not tag or tag in {'context', 'schemaRef', 'unit', 'entity',
                                   'identifier', 'period', 'startDate', 'endDate', 'instant'}:
                continue

            text = (el.text or '').strip()
            if not text:
                continue

            try:
                val = float(text.replace(',', ''))
            except Exception:
                continue

            ctx_ref = el.attrib.get('contextRef')
            period_end = ctx_end.get(ctx_ref, 'unknown')

            # Map to canonical key
            canonical = self._map_to_canonical(tag)
            if not canonical:
                continue

            bucket = facts_by_period.setdefault(period_end, {})
            if canonical not in bucket:
                bucket[canonical] = val

        # Convert to records
        for period_end, facts in facts_by_period.items():
            if period_end == 'unknown':
                continue
            record = self._facts_to_record(company_number, period_end, facts)
            records.append(record)

        # Debug logging
        if not records:
            self.log_callback(f"No records extracted from {filename}. Company: {company_number}, Facts by period: {len(facts_by_period)}, Contexts: {len(ctx_end)}")

        return records

    def _parse_ixbrl_bytes(self, data: bytes, filename: str) -> list[dict]:
        """
        Parse iXBRL HTML data from bytes.

        Returns list of financial records.
        """
        # Try to extract company number from filename first
        company_number = self._extract_company_number(filename)

        records = []
        try:
            text = data.decode('utf-8', errors='ignore')
        except Exception as e:
            print(f"Warning: iXBRL decode failed for {filename}: {e}")
            return []

        # If filename didn't have company number, try to extract from iXBRL content
        if not company_number:
            company_number = self._extract_company_number_from_ixbrl(text)
            if company_number:
                self.log_callback(f"Extracted company number {company_number} from iXBRL content in {filename}")

        if not company_number:
            self.log_callback(f"Skipping {filename}: No company number found in filename or iXBRL content")
            return []

        # Build context mapping
        ctx_end: dict[str, str] = {}
        for m in IX_CONTEXT_RE.finditer(text):
            ctx_id = m.group('id')
            end_val = m.group('end') or m.group('inst')
            if ctx_id and end_val:
                ctx_end[ctx_id] = end_val

        # Collect facts by period
        facts_by_period: dict[str, dict[str, float]] = {}

        # Try both patterns for nonFraction
        matches = []
        matches.extend(IX_NONFRACTION_RE_1.finditer(text))
        matches.extend(IX_NONFRACTION_RE_2.finditer(text))
        
        # Debug: count total nonFraction elements
        total_nonfraction = len(matches)

        for m in matches:
            name = m.group('name')
            ctx = m.group('context')
            raw = re.sub(r'<.*?>', '', m.group('value') or '').strip()

            try:
                val = float(raw.replace(',', ''))
            except Exception:
                continue

            period_end = ctx_end.get(ctx, 'unknown')
            tag = self._localname(name)
            canonical = self._map_to_canonical(tag)
            if not canonical:
                continue

            bucket = facts_by_period.setdefault(period_end, {})
            if canonical not in bucket:
                bucket[canonical] = val

        # Convert to records
        for period_end, facts in facts_by_period.items():
            if period_end == 'unknown':
                continue
            record = self._facts_to_record(company_number, period_end, facts)
            records.append(record)

        # Debug logging
        if not records:
            self.log_callback(f"No records extracted from {filename}. Company: {company_number}, Total nonFraction tags: {total_nonfraction}, Facts by period: {len(facts_by_period)}, Contexts: {len(ctx_end)}")

        return records

    def _localname(self, tag: str) -> str:
        """Extract local name from qualified tag."""
        if '}' in tag:
            return tag.split('}', 1)[1]
        if ':' in tag:
            return tag.split(':', 1)[1]
        return tag

    def _map_to_canonical(self, tag: str) -> str | None:
        """Map XBRL tag to canonical field name (case-insensitive with namespace support)."""
        tag_lower = tag.lower()
        for key, aliases in TAG_SYNONYMS.items():
            for alias in aliases:
                alias_lower = alias.lower()
                # Exact match (case-insensitive)
                if tag_lower == alias_lower:
                    return key
                # Match with namespace prefix (e.g., "uk-gaap_Turnover" matches "Turnover")
                if tag_lower.endswith('_' + alias_lower) or tag_lower.endswith(':' + alias_lower):
                    return key
                # Match if alias is the suffix after underscore or colon
                if '_' in tag_lower and tag_lower.split('_')[-1] == alias_lower:
                    return key
                if ':' in tag_lower and tag_lower.split(':')[-1] == alias_lower:
                    return key
        return None

    def _facts_to_record(self, company_number: str, period_end: str, facts: dict) -> dict:
        """Convert extracted facts to a staging_financials record."""
        # Calculate net_worth if we have assets and liabilities
        net_worth = None
        if 'total_assets' in facts and 'total_liabilities' in facts:
            net_worth = facts['total_assets'] - facts['total_liabilities']
        elif 'net_assets' in facts:
            net_worth = facts['net_assets']

        return {
            'company_number': company_number,
            'period_end': period_end,
            'period_start': None,  # Not always available in bulk data
            'turnover': facts.get('turnover'),
            'profit_loss': facts.get('profit_loss') or facts.get('operating_profit'),
            'total_assets': facts.get('total_assets'),
            'total_liabilities': facts.get('total_liabilities'),
            'net_worth': net_worth,
            'source': 'bulk_xbrl',
            'raw_data': facts,
        }

    def _compute_row_hash(self, row: pd.Series) -> str:
        """Compute MD5 hash for change detection."""
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
        """Compute MD5 hash from a dictionary of field values."""
        values = []
        for field in cls.HASH_FIELDS:
            val = data.get(field, '')
            if val is None or (isinstance(val, float) and pd.isna(val)):
                val = ''
            values.append(str(val))

        hash_string = '|'.join(values)
        return hashlib.md5(hash_string.encode('utf-8')).hexdigest()
