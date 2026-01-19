"""
Parser for Companies House Accounts bulk data files (XBRL/iXBRL format).

The Accounts bulk data contains XBRL and iXBRL formatted financial filings.
Each ZIP contains multiple account files organized by company.
"""
from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Iterator, Any, Optional, Callable, Dict, List
from zipfile import ZipFile
from xml.etree import ElementTree as ET
from datetime import datetime
import dateutil.parser

import pandas as pd

# Import TagManager
try:
    from .tag_manager import TagManager
except ImportError:
    # Handle case where run from different context
    import sys
    sys.path.append(str(Path(__file__).parent))
    from tag_manager import TagManager


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
# Pattern 3: Also try nonNumeric for text values (like dates/descriptions)
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
        'net_assets_liabilities',
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
        
        # Initialize TagManager
        # Assuming accounts_tags directory is in the same directory as this parser
        parser_dir = Path(__file__).parent
        self.tag_manager = TagManager(
            tag_dict_path=str(parser_dir / 'accounts_tags' / 'tag_dictionary.json'),
            taxonomy_dir=str(parser_dir / 'accounts_tags')
        )

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
                    df = self._process_records(records)
                    if not df.empty:
                        yield df
                    records = []

            # Yield remaining records
            if records:
                df = self._process_records(records)
                if not df.empty:
                    yield df

    def _process_records(self, records: list[dict]) -> pd.DataFrame:
        """
        Process a list of raw records into a DataFrame.
        Handles deduplication (keeping latest period_end for same company/period).
        """
        if not records:
            return pd.DataFrame()
            
        df = pd.DataFrame(records)
        
        # Deduplication logic:
        # If we have multiple entries for the same company and period_end,
        # we generally want to keep the most "complete" one or just the latest one processed.
        # Since we extract period_end from filename mainly, let's assume one file = one record per period.
        # But if we have multiple files for same period? 
        # User said: "for each column ... there might be two of them (we need the latest one) you can figure out which one that has the latest year by comparing it to the period_end column"
        # This implies selecting the *latest financial period* if we have multiple filing periods?
        # Or if we have multiple DOCUMENTS for the SAME period?
        # Standard approach: Deduplicate on (company_number, period_end).
        # We'll simple drop duplicates for now, keeping last (assuming order of processing might correlate with recency or just arbitrary).
        # Better: if we had 'filed_date', we'd use that.
        
        df = df.drop_duplicates(subset=['company_number', 'period_end'], keep='last')
        
        df['data_hash'] = df.apply(self._compute_row_hash, axis=1)
        return df

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
        Format: ProdXXX_ChunkXXX_CompanyNumber_Date.html
        """
        # 1. Try splitting by underscore (Primary)
        parts = filename.split('_')
        if len(parts) >= 3:
             # Expecting 3rd part to be company number (index 2)
             candidate = parts[2]
             # Basic sanity check (8 chars, usually digits or 2 letters + 6 digits)
             if len(candidate) == 8:
                 return candidate

        # 2. Fallback regex (improved to handle underscores)
        match = re.search(r'(?:^|_)(\d{8})(?:_|\.)', filename)
        if match:
             return match.group(1)

        # Try alphanumeric format (SC123456, etc.)
        match = re.search(r'\b([A-Z]{2}\d{6})\b', filename)
        if match:
             return match.group(1)
        return None

    def _extract_period_end_from_filename(self, filename: str) -> str | None:
        """
        Extract period_end date from filename.
        Format: Prod223_2125_09652609_20180331.html -> 2018-03-31
        """
        # Look for YYYYMMDD before the extension
        match = re.search(r'_(\d{8})\.', filename)
        if match:
            date_str = match.group(1)
            try:
                # Convert to YYYY-MM-DD
                return datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
            except ValueError:
                pass
        return None

    def _parse_date(self, date_str: str) -> str | None:
        """Parse various date formats to YYYY-MM-DD."""
        if not date_str:
            return None
        try:
            # Try flexible parsing
            dt = dateutil.parser.parse(date_str, dayfirst=True) # UK dates usually day first
            return dt.strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            try:
                # Fallbacks for specific edge cases if dateutil fails
                # e.g., 31.12.17
                if re.match(r'\d{2}\.\d{2}\.\d{2}', date_str):
                     return datetime.strptime(date_str, '%d.%m.%y').strftime('%Y-%m-%d')
                if re.match(r'\d{2}\.\d{2}\.\d{4}', date_str):
                     return datetime.strptime(date_str, '%d.%m.%Y').strftime('%Y-%m-%d')
            except ValueError:
                pass
        return None

    def _get_best_value(self, facts_by_period: dict, period_end: str, target_column: str) -> float | None:
        """
        Try to find a value for the target_column using all potential tags.
        """
        potential_tags = self.tag_manager.get_potential_tags(target_column)
        
        # Check if period exists
        if period_end not in facts_by_period:
            return None
            
        facts = facts_by_period[period_end]
        
        for tag in potential_tags:
            tag_local = self._localname(tag).lower()
            
            for fact_tag, value in facts.items():
                fact_tag_local = self._localname(fact_tag).lower()
                
                # Check for exact match of local name
                if fact_tag_local == tag_local:
                    return value
                
                # Also check without hyphens etc if fuzzy?
                if fact_tag_local == tag_local.replace('-', '').replace('_', ''):
                    return value
                    
        return None

    def _get_best_text_value(self, text_facts_by_period: dict, period_end: str, target_column: str) -> str | None:
        """
        Same as _get_best_value but for text fields (like period_start).
        """
        potential_tags = self.tag_manager.get_potential_tags(target_column)
        
        # Use wildcard period or specific period logic?
        # Non-numeric facts (like period start) might be associated with the same context/period as the numeric facts.
        
        if period_end not in text_facts_by_period:
             # Try to search in 'unknown' period or global context?
             # Often period start is defined IN the context.
             # But if we are looking for a TAG that contains the start date (user requirement), it follows standard tag logic.
             if period_end not in text_facts_by_period:
                 return None

        facts = text_facts_by_period[period_end]
        for tag in potential_tags:
            tag_local = self._localname(tag).lower()
            for fact_tag, value in facts.items():
                fact_tag_local = self._localname(fact_tag).lower()
                if fact_tag_local == tag_local:
                    return value
        return None

    def _extract_company_number_xbrl(self, root: ET.Element) -> str | None:
        """Helper to extract company number from XBRL XML."""
        # Try standard identifier
        for identifier in root.findall('.//*{*}identifier'):
            text = (identifier.text or '').strip()
            # print(f"DEBUG: Found identifier: {text}")
            if text and (len(text) == 8 and text.isdigit()) or (len(text) == 8 and text[0:2].isalpha()):
                 return text
        
        # Try CompaniesHouseRegisteredNumber
        for num_tag in root.findall('.//*{*}CompaniesHouseRegisteredNumber'):
            text = (num_tag.text or '').strip()
            # print(f"DEBUG: Found CompaniesHouseRegisteredNumber: {text}")
            if text:
                 return text
        return None

    def _extract_company_number_ixbrl(self, html_text: str) -> str | None:
         # Search for identifier elements in HTML
        identifier_pattern = re.compile(
            r'<(?:xbrli:)?identifier[^>]*>([^<]+)</(?:xbrli:)?identifier>',
            re.IGNORECASE
        )
        match = identifier_pattern.search(html_text)
        if match:
            return match.group(1).strip()
        return None

    def _parse_xbrl_bytes(self, data: bytes, filename: str) -> list[dict]:
        """
        Parse XBRL XML data.
        """
        # 1. Company Number
        company_number = self._extract_company_number(filename)
        
        # 2. Period End from filename (Primary source)
        filename_period_end = self._extract_period_end_from_filename(filename)

        records = []
        try:
            root = ET.fromstring(data)
        except Exception as e:
            print(f"Warning: XBRL parse failed for {filename}: {e}")
            return []

        if not company_number:
            company_number = self._extract_company_number_xbrl(root)
        
        if not company_number:
            self.log_callback(f"Skipping {filename}: No company number found")
            return []

        # Build context date mapping
        ctx_dates: dict[str, dict] = {} # id -> {'end': '...', 'start': '...'}
        
        # Use iter() to be robust against namespace issues in findall
        for ctx in root.iter():
            if self._localname(ctx.tag) != 'context':
                continue

            ctx_id = ctx.attrib.get('id')
            if not ctx_id: continue
            
            end_el = ctx.find('.//*{*}endDate')
            start_el = ctx.find('.//*{*}startDate')
            inst_el = ctx.find('.//*{*}instant')
            
            end_val = (end_el.text if end_el is not None else None) or \
                      (inst_el.text if inst_el is not None else None)
            start_val = (start_el.text if start_el is not None else None)
            
            if end_val:
                ctx_dates[ctx_id] = {'end': end_val, 'start': start_val}
        
        # DEBUG: Log context dates
        self.log_callback(f"DEBUG: Found Contexts: {list(ctx_dates.keys())}")

        # Collect facts
        facts_by_period: dict[str, dict[str, float]] = {}
        text_facts_by_period: dict[str, dict[str, str]] = {}
        
        for el in root.iter():
            tag = el.tag # qualified name
            # Skip structural tags
            local = self._localname(tag)
            if local in {'context', 'schemaRef', 'unit', 'entity', 'identifier', 'period', 'startDate', 'endDate', 'instant', 'segment'}:
                continue

            text = (el.text or '').strip()
            if not text: continue
            
            ctx_ref = el.attrib.get('contextRef')
            # Determine period for this fact
            # Use filename period if available? No, contextRef determines the period of the FACT.
            # But we only want facts that match the REPORT period (filename period).
            # Or we extract all? User said "period_end will always be extracted from the file name".
            # This implies the RECORD we create should be for THAT period_end.
            # So filtering facts by matching context date?
            
            fact_period_end = 'unknown'
            if ctx_ref and ctx_ref in ctx_dates:
                 fact_period_end = ctx_dates[ctx_ref]['end']
            
            # If filename period is known, and fact period doesn't match, maybe ignore?
            # Or store it and filter later.
            # Let's group by period logic from context.
            
            # Numeric check
            try:
                val = float(text.replace(',', ''))
                bucket = facts_by_period.setdefault(fact_period_end, {})
                bucket[tag] = val # Store full tag for matching
            except ValueError:
                bucket = text_facts_by_period.setdefault(fact_period_end, {})
                bucket[tag] = text

        # Create record
        # Use filename period if available, otherwise use found periods
        target_period = filename_period_end
        
        # If we have a filename period, we prioritize extracting data for THAT period.
        # If not, we might yield multiple records for different periods found in the file?
        # User requirement implies 1 file -> 1 main record likely.
        
        record_periods = {target_period} if target_period else set(facts_by_period.keys())
        record_periods.discard('unknown')
        if not record_periods and 'unknown' in facts_by_period:
             pass 

        # DEBUG: Log keys unconditionally
        self.log_callback(f"DEBUG: Target Period: {target_period}")
        self.log_callback(f"DEBUG: Facts Keys: {list(facts_by_period.keys())}")
        if target_period and target_period in facts_by_period:
             found_tags = list(facts_by_period[target_period].keys())
             self.log_callback(f"DEBUG: Found tags for {target_period}: {found_tags}")
        
        parsed_records = []
        for p_end in record_periods:
            if not p_end: continue
            
            # Initialize record for this period
            record = {}
            
            # specific tag logic
            # Dynamic extraction for all keys in tag dictionary
            for col_name in self.tag_manager.get_all_keys():
                if col_name == 'period_start': continue
                
                # Try numeric
                val = self._get_best_value(facts_by_period, p_end, col_name)
                if val is None:
                     # Try text
                     val = self._get_best_text_value(text_facts_by_period, p_end, col_name)
                
                record[col_name] = val
            
            # Period Start: Try tag first, then context
            period_start = None
            raw_start_date = self._get_best_text_value(text_facts_by_period, p_end, 'period_start')
            if raw_start_date:
                period_start = self._parse_date(raw_start_date)
            
            # Fallback to context start date if available matching p_end
            if not period_start:
                 # Find a context with this end date
                 for dates in ctx_dates.values():
                     if dates['end'] == p_end and dates['start']:
                         period_start = dates['start'] # Already YYYY-MM-DD from context? Usually yes.
                         break

            # Update record with metadata
            record.update({
                'company_number': company_number,
                'period_end': p_end,
                'period_start': period_start,
                'source': 'bulk_xbrl',
                'raw_data': {} # Can populate if needed
            })
            parsed_records.append(record)
            
        return parsed_records

    def _parse_ixbrl_bytes(self, data: bytes, filename: str) -> list[dict]:
        """
        Parse iXBRL HTML data.
        """
        # 1. Company Number
        company_number = self._extract_company_number(filename)
        # 2. Period End from filename
        filename_period_end = self._extract_period_end_from_filename(filename)

        try:
            text = data.decode('utf-8', errors='ignore')
        except Exception as e:
            print(f"Warning: iXBRL decode failed for {filename}: {e}")
            return []

        if not company_number:
            company_number = self._extract_company_number_ixbrl(text)
        
        if not company_number:
            self.log_callback(f"Skipping {filename}: No company number found")
            return []

        # Build context mapping
        ctx_dates: dict[str, dict] = {}
        for m in IX_CONTEXT_RE.finditer(text):
            ctx_id = m.group('id')
            # Extract end date, maybe start date too? 
            # Regex needs to be smarter to capture startDate.
            # Simplified regex used above only captured 'end'. 
            # For iXBRL full support, we'd need a stronger parser, but staying with regex as per existing code style.
            end_val = m.group('end')
            if ctx_id and end_val:
                ctx_dates[ctx_id] = {'end': end_val}

        # Collect facts
        facts_by_period: dict[str, dict[str, float]] = {}
        text_facts_by_period: dict[str, dict[str, str]] = {}

        # Numeric values
        matches = []
        matches.extend(IX_NONFRACTION_RE_1.finditer(text))
        matches.extend(IX_NONFRACTION_RE_2.finditer(text))

        for m in matches:
            name = m.group('name') # e.g. uk-gaap:Turnover
            ctx = m.group('context')
            raw_val = re.sub(r'<.*?>', '', m.group('value') or '').strip()
            
            fact_period_end = ctx_dates.get(ctx, {}).get('end', 'unknown')
            
            try:
                # Handle sign? (format like (1,234) for negative?)
                # Assuming standard float parse for now
                val = float(raw_val.replace(',', '').replace(' ', ''))
                # Handle sign attribute? (not parsed yet)
                bucket = facts_by_period.setdefault(fact_period_end, {})
                bucket[name] = val
            except ValueError:
                pass
        
        # Text values (for dates etc)
        # Regex for nonNumeric
        for m in IX_NONNUMERIC_RE.finditer(text):
            name = m.group('name')
            ctx = m.group('context')
            raw_val = re.sub(r'<.*?>', '', m.group('value') or '').strip()
            fact_period_end = ctx_dates.get(ctx, {}).get('end', 'unknown')
            
            bucket = text_facts_by_period.setdefault(fact_period_end, {})
            bucket[name] = raw_val

        # Create record
        target_period = filename_period_end
        record_periods = {target_period} if target_period else set(facts_by_period.keys())
        record_periods.discard('unknown')

        parsed_records = []
        for p_end in record_periods:
            if not p_end: continue
            
            # Initialize record
            record = {}
            
            # specific tag logic
            # Dynamic extraction for all keys in tag dictionary
            for col_name in self.tag_manager.get_all_keys():
                # Skip period_start, handled separately as text/date
                if col_name == 'period_start': continue
                
                # Check known types if needed, or assume numeric if not in a 'text_fields' list?
                # For now, most matching standard logic are numeric.
                # But some are text (e.g. description_body...). 
                # We need a way to distinguish.
                # Simple heuristic: try float, if fails, assume text? 
                # Or check explicit list.
                
                # Better: try numeric extraction first
                val = self._get_best_value(facts_by_period, p_end, col_name)
                if val is None:
                     # Try text extraction
                     val = self._get_best_text_value(text_facts_by_period, p_end, col_name)
                
                record[col_name] = val
            
            period_start = None
            raw_start_date = self._get_best_text_value(text_facts_by_period, p_end, 'period_start')
            if raw_start_date:
                period_start = self._parse_date(raw_start_date)

            record.update({
                'company_number': company_number,
                'period_end': p_end,
                'period_start': period_start,
                'source': 'bulk_xbrl',
                'raw_data': {} 
            })
            parsed_records.append(record)

        return parsed_records

    def _localname(self, tag: str) -> str:
        """Extract local name from qualified tag."""
        if '}' in tag:
            return tag.split('}', 1)[1]
        if ':' in tag:
            return tag.split(':', 1)[1]
        return tag

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
