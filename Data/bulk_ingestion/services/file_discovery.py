"""
File discovery service for Companies House bulk data products.

Scrapes the Companies House download pages to find available files,
caches results for session efficiency.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
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
        'company': '/en_output.html',
        'psc': '/en_pscdata.html',
        'accounts': '/en_accountsdata.html',
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
            monthly_only: If True, only return one snapshot per month (earliest available)

        Returns:
            List of AvailableFile objects within the date range
        """
        if product not in self.PRODUCT_PAGES:
            raise ValueError(f"Unknown product: {product}. Must be one of {list(self.PRODUCT_PAGES.keys())}")

        # Get all files for this product (cached)
        all_files = self._get_all_files(product)

        # Filter by date range
        filtered = [
            f for f in all_files
            if start_date <= f.file_date <= end_date
        ]

        # Filter to monthly only if requested (select earliest snapshot per month)
        if monthly_only and product in ('psc', 'accounts'):
            monthly_files = {}
            for f in filtered:
                month_key = (f.file_date.year, f.file_date.month)
                if month_key not in monthly_files or f.file_date < monthly_files[month_key].file_date:
                    monthly_files[month_key] = f
            filtered = list(monthly_files.values())

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
        Scrape a Companies House download page for available files.

        Args:
            product: Product name

        Returns:
            List of AvailableFile objects found on the page
        """
        url = f"{self.BASE_URL}{self.PRODUCT_PAGES[product]}"

        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Warning: Failed to fetch {url}: {e}")
            return []

        soup = BeautifulSoup(response.text, 'html.parser')
        files = []

        # Find all links on the page
        for link in soup.find_all('a', href=True):
            href = link['href']

            # Make URL absolute if needed
            if not href.startswith('http'):
                href = f"{self.BASE_URL}/{href.lstrip('/')}"

            # Parse based on product type
            file_info = self._parse_file_url(product, href)
            if file_info:
                files.append(file_info)

        return files

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
            match = self.ACCOUNTS_PATTERN.search(url)
            if match:
                file_date = datetime.strptime(match.group(1), '%Y-%m-%d').date()
                return AvailableFile(
                    product='accounts',
                    url=url,
                    file_date=file_date,
                )

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
