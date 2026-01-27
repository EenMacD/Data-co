"""
Download manager for concurrent file downloads with progress tracking.
"""
from __future__ import annotations

import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional
from threading import Lock

import requests


@dataclass
class DownloadProgress:
    """Tracks progress of a download."""
    url: str
    total_bytes: int = 0
    downloaded_bytes: int = 0
    status: str = 'pending'  # pending, downloading, completed, failed
    error: Optional[str] = None
    local_path: Optional[Path] = None

    @property
    def progress_percent(self) -> float:
        """Return download progress as percentage."""
        if self.total_bytes == 0:
            return 0.0
        return (self.downloaded_bytes / self.total_bytes) * 100


@dataclass
class DownloadResult:
    """Result of a download operation."""
    url: str
    success: bool
    local_path: Optional[Path] = None
    error: Optional[str] = None
    bytes_downloaded: int = 0


class DownloadManager:
    """
    Manages concurrent file downloads with progress tracking.

    Features:
    - Concurrent downloads with configurable workers
    - Progress callbacks for real-time updates
    - Automatic cleanup of failed downloads
    - Resume support (not implemented yet, but designed for it)
    """

    CHUNK_SIZE = 8192  # 8KB chunks

    def __init__(
        self,
        download_dir: Optional[Path] = None,
        max_workers: int = 3,
        session: Optional[requests.Session] = None
    ):
        """
        Initialize the download manager.

        Args:
            download_dir: Directory to save downloads. Uses temp dir if None.
            max_workers: Maximum concurrent downloads
            session: Optional requests session for connection pooling
        """
        self.download_dir = Path(download_dir) if download_dir else Path(tempfile.mkdtemp())
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.max_workers = max_workers
        self.session = session or requests.Session()
        self._progress: dict[str, DownloadProgress] = {}
        self._lock = Lock()
        self._executor: Optional[ThreadPoolExecutor] = None

    def download_file(
        self,
        url: str,
        filename: Optional[str] = None,
        progress_callback: Optional[Callable[[DownloadProgress], None]] = None
    ) -> DownloadResult:
        """
        Download a single file synchronously.

        Args:
            url: URL to download
            filename: Optional filename (extracted from URL if not provided)
            progress_callback: Optional callback for progress updates

        Returns:
            DownloadResult with success status and local path
        """
        if filename is None:
            filename = url.split('/')[-1]

        local_path = self.download_dir / filename

        # Initialize progress tracking
        progress = DownloadProgress(url=url, status='downloading')
        self._progress[url] = progress

        try:
            # Make request with streaming
            response = self.session.get(url, stream=True, timeout=60)
            response.raise_for_status()

            # Get total size if available
            total_size = int(response.headers.get('content-length', 0))
            progress.total_bytes = total_size

            # Download with progress tracking
            downloaded = 0
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=self.CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        progress.downloaded_bytes = downloaded

                        if progress_callback:
                            progress_callback(progress)

            progress.status = 'completed'
            progress.local_path = local_path

            if progress_callback:
                progress_callback(progress)

            return DownloadResult(
                url=url,
                success=True,
                local_path=local_path,
                bytes_downloaded=downloaded
            )

        except Exception as e:
            progress.status = 'failed'
            progress.error = str(e)

            # Clean up partial download
            if local_path.exists():
                local_path.unlink()

            if progress_callback:
                progress_callback(progress)

            return DownloadResult(
                url=url,
                success=False,
                error=str(e)
            )

    def download_files(
        self,
        urls: list[str],
        progress_callback: Optional[Callable[[str, DownloadProgress], None]] = None
    ) -> list[DownloadResult]:
        """
        Download multiple files concurrently.

        Args:
            urls: List of URLs to download
            progress_callback: Optional callback(url, progress) for updates

        Returns:
            List of DownloadResult objects
        """
        results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures: dict[Future, str] = {}

            for url in urls:
                # Create a wrapper callback that includes the URL
                def make_callback(u: str):
                    def callback(p: DownloadProgress):
                        if progress_callback:
                            progress_callback(u, p)
                    return callback

                future = executor.submit(
                    self.download_file,
                    url,
                    progress_callback=make_callback(url)
                )
                futures[future] = url

            # Collect results as they complete
            for future in futures:
                url = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    results.append(DownloadResult(url=url, success=False, error=str(e)))

        return results

    def get_progress(self, url: str) -> Optional[DownloadProgress]:
        """Get current progress for a download."""
        return self._progress.get(url)

    def get_all_progress(self) -> dict[str, DownloadProgress]:
        """Get progress for all tracked downloads."""
        return dict(self._progress)

    def cleanup(self, urls: Optional[list[str]] = None) -> None:
        """
        Clean up downloaded files.

        Args:
            urls: Specific URLs to clean up. Cleans all if None.
        """
        if urls is None:
            urls = list(self._progress.keys())

        for url in urls:
            progress = self._progress.get(url)
            if progress and progress.local_path and progress.local_path.exists():
                try:
                    progress.local_path.unlink()
                except Exception:
                    pass

        # Remove from tracking
        for url in urls:
            self._progress.pop(url, None)

    def cleanup_file(self, local_path: Path) -> None:
        """Delete a specific downloaded file."""
        if local_path.exists():
            try:
                local_path.unlink()
            except Exception:
                pass
