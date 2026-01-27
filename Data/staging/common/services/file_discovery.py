"""
File discovery service for Companies House bulk data products.

Scrapes the Companies House download pages to find available files,
caches results for session efficiency.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional
from functools import lru_cache

import requests
from bs4 import BeautifulSoup


@dataclass
class AvailableFile:
    """Represents a downloadable file from Companies House."""
    product: str  # 'company', 'psc', 'accounts'
    url: str
    file_date: date
    part: Optional[int] = None
    total_parts: Optional[int] = None
    size_mb: Optional[float] = None
    is_monthly_archive: bool = False

    @property
    def filename(self) -> str:
        """Extract filename from URL."""
        return self.url.split('/')[-1]

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            'product': self.product,
            'url': self.url,
            'filename': self.filename,
            'date': self.file_date.isoformat(),
            'part': self.part,
            'total_parts': self.total_parts,
            'size_mb': self.size_mb,
            'is_monthly_archive': self.is_monthly_archive,
        }


class FileDiscoveryService:
    """
    Discovers available bulk data files from Companies House download pages.

    Scrapes:
    - http://download.companieshouse.gov.uk/en_output.html (Company data)
    - http://download.companieshouse.gov.uk/en_pscdata.html (PSC data)
    - http://download.companieshouse.gov.uk/en_accountsdata.html (Accounts data)
    """

    BASE_URL = 'http://download.companieshouse.gov.uk'

    PRODUCT_PAGES = {
        'company': ['/en_output.html'],
        'psc': ['/en_pscdata.html'],
        'accounts': [
            '/en_accountsdata.html',
            '/en_monthlyaccountsdata.html',
            '/historicmonthlyaccountsdata.html'
        ],
    }

    # Regex patterns for extracting file info from URLs
    COMPANY_PATTERN = re.compile(
        r'BasicCompanyData-(\d{4}-\d{2}-\d{2})-part(\d+)_(\d+)\.zip',
        re.IGNORECASE
    )
    PSC_PATTERN = re.compile(
        r'psc-snapshot-(\d{4}-\d{2}-\d{2})_(\d+)of(\d+)\.zip',
        re.IGNORECASE
    )
    ACCOUNTS_PATTERN = re.compile(
        r'Accounts_Bulk_Data-(\d{4}-\d{2}-\d{2})\.zip',
        re.IGNORECASE
    )
    ACCOUNTS_MONTHLY_PATTERN = re.compile(
        r'Accounts_Monthly_Data-([a-zA-Z]+)(\d{4})\.zip',
        re.IGNORECASE
    )

    def __init__(self, session: Optional[requests.Session] = None):
        """
        Initialize the file discovery service.

        Args:
            session: Optional requests session for connection pooling
        """
        self.session = session or requests.Session()
        self._cache: dict[str, list[AvailableFile]] = {}

    def discover_files(
        self,
        product: str,
        start_date: date,
        end_date: date,
        monthly_only: bool = True
    ) -> list[AvailableFile]:
        """
        Discover available files for a product within a date range.

        Args:
            product: 'company', 'psc', or 'accounts'
            start_date: Start of date range
            end_date: End of date range
            monthly_only: If True, only return one snapshot per month.
                         For 'accounts', prefers Monthly Archives and drops older Daily files.

        Returns:
            List of AvailableFile objects within the date range
        """
        if product not in self.PRODUCT_PAGES:
            raise ValueError(f"Unknown product: {product}. Must be one of {list(self.PRODUCT_PAGES.keys())}")

        # Get all files for this product (cached)
        all_files = self._get_all_files(product)

        # Filter by date range first
        filtered = [
            f for f in all_files
            if start_date <= f.file_date <= end_date
        ]

        # Accounts specific logic: Prefer Monthly Archives over Daily files
        if product == 'accounts':
            archives = [f for f in filtered if f.is_monthly_archive]
            dailies = [f for f in filtered if not f.is_monthly_archive]
            
            # Find the coverage of the latest archive
            latest_archive_end_date = date.min
            if archives:
                # Assuming archive date is 1st of month, it covers the whole month
                # Find the latest archive date
                latest_archive_start = max(f.file_date for f in archives)
                
                # Calculate end of that month
                if latest_archive_start.month == 12:
                    latest_archive_end_date = date(latest_archive_start.year, 12, 31)
                else:
                    # Start of next month - 1 day
                    next_month = date(latest_archive_start.year, latest_archive_start.month + 1, 1)
                    latest_archive_end_date = next_month - timedelta(days=1)

            # Simple check: Keep daily ONLY if it is newer than the latest archive month end.
            kept_dailies = [d for d in dailies if d.file_date > latest_archive_end_date]
            
            filtered = archives + kept_dailies

        # PSC Monthly Only logic (One snapshot per month)
        elif monthly_only and product == 'psc':
            # First, determine the target date (earliest available) for each month
            target_dates: dict[tuple[int, int], date] = {}
            for f in filtered:
                month_key = (f.file_date.year, f.file_date.month)
                if month_key not in target_dates or f.file_date < target_dates[month_key]:
                    target_dates[month_key] = f.file_date
            
            # Then keep all files that match the target date for their month
            filtered = [
                f for f in filtered
                if f.file_date == target_dates[(f.file_date.year, f.file_date.month)]
            ]

        return sorted(filtered, key=lambda f: (f.file_date, f.part or 0))

    def _get_all_files(self, product: str) -> list[AvailableFile]:
        """
        Get all available files for a product, with caching.

        Args:
            product: Product name

        Returns:
            List of all available files
        """
        if product in self._cache:
            return self._cache[product]

        files = self._scrape_product_page(product)
        self._cache[product] = files
        return files

    def _scrape_product_page(self, product: str) -> list[AvailableFile]:
        """
        Scrape Companies House download pages for available files.

        Args:
            product: Product name

        Returns:
            List of AvailableFile objects found on the pages
        """
        endpoints = self.PRODUCT_PAGES[product]
        all_files = []
        seen_urls = set()
        seen_filenames = set()

        for endpoint in endpoints:
            url = f"{self.BASE_URL}{endpoint}"

            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
            except requests.RequestException as e:
                print(f"Warning: Failed to fetch {url}: {e}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all links on the page
            for link in soup.find_all('a', href=True):
                href = link['href']

                # Make URL absolute if needed
                if not href.startswith('http'):
                    href = f"{self.BASE_URL}/{href.lstrip('/')}"

                # Skip if we've already seen this exact URL
                if href in seen_urls:
                    continue

                # Parse based on product type
                file_info = self._parse_file_url(product, href)
                if file_info:
                    # Deduplicate by filename as well (handles archive vs root paths)
                    if file_info.filename in seen_filenames:
                        continue
                        
                    all_files.append(file_info)
                    seen_urls.add(href)
                    seen_filenames.add(file_info.filename)

        return all_files

    def _parse_file_url(self, product: str, url: str) -> Optional[AvailableFile]:
        """
        Parse a URL to extract file information.

        Args:
            product: Product type
            url: URL to parse

        Returns:
            AvailableFile if URL matches expected pattern, None otherwise
        """
        if product == 'company':
            match = self.COMPANY_PATTERN.search(url)
            if match:
                file_date = datetime.strptime(match.group(1), '%Y-%m-%d').date()
                return AvailableFile(
                    product='company',
                    url=url,
                    file_date=file_date,
                    part=int(match.group(2)),
                    total_parts=int(match.group(3)),
                )

        elif product == 'psc':
            match = self.PSC_PATTERN.search(url)
            if match:
                file_date = datetime.strptime(match.group(1), '%Y-%m-%d').date()
                return AvailableFile(
                    product='psc',
                    url=url,
                    file_date=file_date,
                    part=int(match.group(2)),
                    total_parts=int(match.group(3)),
                )

        elif product == 'accounts':
            # Check for daily file pattern
            match = self.ACCOUNTS_PATTERN.search(url)
            if match:
                file_date = datetime.strptime(match.group(1), '%Y-%m-%d').date()
                return AvailableFile(
                    product='accounts',
                    url=url,
                    file_date=file_date,
                    is_monthly_archive=False,
                )
            
            # Check for monthly/historic file pattern
            match = self.ACCOUNTS_MONTHLY_PATTERN.search(url)
            if match:
                try:
                    month_str = match.group(1)
                    year_str = match.group(2)
                    # Parse MonthNameYYYY (e.g. 'January2024')
                    dt = datetime.strptime(f"{month_str}{year_str}", '%B%Y')
                    return AvailableFile(
                        product='accounts',
                        url=url,
                        file_date=dt.date(),
                        is_monthly_archive=True,
                    )
                except ValueError:
                   # Failed to parse date
                   return None

        return None

    def clear_cache(self) -> None:
        """Clear the file cache."""
        self._cache.clear()

    def get_available_dates(self, product: str) -> list[date]:
        """
        Get all unique dates for which files are available.

        Args:
            product: Product name

        Returns:
            Sorted list of dates
        """
        files = self._get_all_files(product)
        dates = set(f.file_date for f in files)
        return sorted(dates)


# Module-level instance for convenience
_default_service: Optional[FileDiscoveryService] = None


def get_file_discovery_service() -> FileDiscoveryService:
    """Get the default file discovery service instance."""
    global _default_service
    if _default_service is None:
        _default_service = FileDiscoveryService()
    return _default_service
