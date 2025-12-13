"""
Ingestion worker for background processing with stop/resume capability.
"""
from __future__ import annotations

import json
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from queue import Queue
from typing import Callable, Optional, Any

# Add parent paths for imports
DATA_ROOT = Path(__file__).resolve().parents[2]  # Data/
if str(DATA_ROOT) not in sys.path:
    sys.path.append(str(DATA_ROOT))

from database.connection import get_staging_db
from .download_manager import DownloadManager
from .bulk_loader import BulkLoader
from parsers import CompanyDataParser, PSCDataParser, AccountsDataParser


@dataclass
class IngestionProgress:
    """Tracks overall ingestion progress."""
    batch_id: str
    status: str = 'pending'  # pending, running, paused, completed, failed
    files_total: int = 0
    files_completed: int = 0
    current_file: Optional[str] = None
    current_file_progress: int = 0  # 0-100
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Stats
    companies_processed: int = 0
    officers_processed: int = 0
    financials_processed: int = 0

    @property
    def overall_progress(self) -> float:
        """Calculate overall progress percentage."""
        if self.files_total == 0:
            return 0.0
        file_progress = (self.files_completed / self.files_total) * 100
        # Add partial progress for current file
        if self.current_file and self.files_total > 0:
            current_contribution = (self.current_file_progress / 100) * (100 / self.files_total)
            file_progress += current_contribution
        return min(file_progress, 100.0)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            'batch_id': self.batch_id,
            'status': self.status,
            'files_total': self.files_total,
            'files_completed': self.files_completed,
            'current_file': self.current_file,
            'current_file_progress': self.current_file_progress,
            'overall_progress': self.overall_progress,
            'error': self.error,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'companies_processed': self.companies_processed,
            'officers_processed': self.officers_processed,
            'financials_processed': self.financials_processed,
        }


class IngestionWorker:
    """
    Background worker for processing file ingestion with stop/resume capability.

    Features:
    - Runs in background thread
    - Progress tracking with callbacks
    - Stop/resume at file boundaries
    - Saves checkpoint to database for resume
    """

    def __init__(
        self,
        log_callback: Optional[Callable[[str], None]] = None,
        progress_callback: Optional[Callable[[IngestionProgress], None]] = None
    ):
        """
        Initialize the ingestion worker.

        Args:
            log_callback: Function to call with log messages
            progress_callback: Function to call with progress updates
        """
        self.log_callback = log_callback or (lambda msg: print(msg))
        self.progress_callback = progress_callback
        self.db = get_staging_db()

        self._thread: Optional[threading.Thread] = None
        self._should_stop = threading.Event()
        self._progress: Optional[IngestionProgress] = None
        self._lock = threading.Lock()

    @property
    def is_running(self) -> bool:
        """Check if worker is currently running."""
        return self._thread is not None and self._thread.is_alive()

    @property
    def progress(self) -> Optional[IngestionProgress]:
        """Get current progress."""
        return self._progress

    def start(self, files: list[dict]) -> str:
        """
        Start processing a list of files.

        Args:
            files: List of file dicts with 'product', 'url', 'file_date', etc.

        Returns:
            Batch ID for tracking this ingestion run
        """
        if self.is_running:
            raise RuntimeError("Worker is already running")

        # Validate all files have required keys before starting
        for idx, file_info in enumerate(files):
            if 'product' not in file_info:
                raise ValueError(
                    f"File at index {idx} is missing required 'product' key. "
                    f"Available keys: {list(file_info.keys())}. File: {file_info}"
                )
            if 'url' not in file_info:
                raise ValueError(
                    f"File at index {idx} is missing required 'url' key. "
                    f"Available keys: {list(file_info.keys())}"
                )

        # Generate batch ID
        batch_id = f"bulk_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

        # Initialize progress
        self._progress = IngestionProgress(
            batch_id=batch_id,
            files_total=len(files),
            started_at=datetime.now()
        )

        # Create ingestion log entry
        self._create_batch_log(batch_id, files)

        # Reset stop flag
        self._should_stop.clear()

        # Start worker thread
        self._thread = threading.Thread(
            target=self._run,
            args=(batch_id, files),
            daemon=True
        )
        self._thread.start()

        return batch_id

    def stop(self) -> None:
        """
        Signal the worker to stop after completing the current file.

        The checkpoint is saved to the database for resume.
        """
        self._should_stop.set()
        self._log("Stop requested - will pause after current file completes")

    def resume(self) -> Optional[str]:
        """
        Resume from the last paused batch.

        Returns:
            Batch ID if resumed, None if no paused batch found
        """
        if self.is_running:
            raise RuntimeError("Worker is already running")

        # Find last paused batch
        batch = self._get_last_paused_batch()
        if not batch:
            return None

        batch_id = batch['batch_id']
        metadata = batch.get('metadata', {})
        files = metadata.get('files', [])
        current_index = metadata.get('current_file_index', 0)

        # Skip completed files
        remaining_files = files[current_index:]

        if not remaining_files:
            self._log(f"No remaining files in batch {batch_id}")
            return None

        # Initialize progress
        self._progress = IngestionProgress(
            batch_id=batch_id,
            files_total=len(files),
            files_completed=current_index,
            started_at=datetime.now()
        )

        # Reset stop flag
        self._should_stop.clear()

        # Update batch status
        self._update_batch_status(batch_id, 'running')

        # Start worker thread
        self._thread = threading.Thread(
            target=self._run,
            args=(batch_id, remaining_files, current_index),
            daemon=True
        )
        self._thread.start()

        return batch_id

    def _run(self, batch_id: str, files: list[dict], start_index: int = 0) -> None:
        """
        Main worker loop - processes files sequentially.

        Args:
            batch_id: Batch identifier
            files: List of files to process
            start_index: Index to start from (for resume)
        """
        self._progress.status = 'running'
        self._notify_progress()

        download_manager = DownloadManager()
        bulk_loader = BulkLoader(batch_id)

        try:
            for idx, file_info in enumerate(files):
                file_index = start_index + idx

                # Check for stop request
                if self._should_stop.is_set():
                    self._log("Stop signal received - pausing")
                    self._save_checkpoint(batch_id, files, file_index)
                    self._progress.status = 'paused'
                    self._update_batch_status(batch_id, 'paused')
                    self._notify_progress()
                    return

                # Validate file_info has required keys
                if 'product' not in file_info:
                    error_msg = (
                        f"File at index {file_index} is missing 'product' key. "
                        f"Available keys: {list(file_info.keys())}. File: {file_info}"
                    )
                    self._log(f"ERROR: {error_msg}")
                    raise KeyError(error_msg)

                if 'url' not in file_info:
                    error_msg = (
                        f"File at index {file_index} is missing 'url' key. "
                        f"Available keys: {list(file_info.keys())}"
                    )
                    self._log(f"ERROR: {error_msg}")
                    raise KeyError(error_msg)

                url = file_info['url']
                product = file_info['product']

                self._progress.current_file = url
                self._progress.current_file_progress = 0
                self._notify_progress()

                self._log(f"[{file_index + 1}/{self._progress.files_total}] Processing {product}: {url}")

                try:
                    # Download file
                    self._log(f"  Downloading...")
                    result = download_manager.download_file(url)

                    if not result.success:
                        self._log(f"  ! Download failed: {result.error}")
                        continue

                    self._progress.current_file_progress = 30
                    self._notify_progress()

                    # Parse and load
                    self._log(f"  Parsing and loading...")
                    local_path = result.local_path

                    if product == 'company':
                        stats = self._process_company_file(local_path, bulk_loader)
                        self._progress.companies_processed += stats.get('inserted', 0)
                    elif product == 'psc':
                        stats = self._process_psc_file(local_path, bulk_loader)
                        self._progress.officers_processed += stats.get('inserted', 0)
                    elif product == 'accounts':
                        stats = self._process_accounts_file(local_path, bulk_loader)
                        self._progress.financials_processed += stats.get('inserted', 0)
                    else:
                        self._log(f"  ! Unknown product type: {product}")
                        stats = {}

                    self._log(f"  Loaded: {stats}")

                    # Cleanup
                    download_manager.cleanup_file(local_path)

                except Exception as e:
                    self._log(f"  ! Error processing file: {e}")
                    import traceback
                    self._log(traceback.format_exc())

                # Mark file complete
                self._progress.files_completed = file_index + 1
                self._progress.current_file_progress = 100
                self._update_batch_progress(batch_id, file_index + 1)
                self._notify_progress()

            # All files completed
            self._progress.status = 'completed'
            self._progress.completed_at = datetime.now()
            self._update_batch_status(batch_id, 'completed')
            self._log(f"Batch {batch_id} completed successfully")
            self._log(f"Stats: {bulk_loader.get_stats()}")

        except Exception as e:
            self._progress.status = 'failed'
            self._progress.error = str(e)
            self._update_batch_status(batch_id, 'failed', str(e))
            self._log(f"Batch {batch_id} failed: {e}")
            import traceback
            self._log(traceback.format_exc())

        finally:
            self._notify_progress()

    def _process_company_file(self, file_path: Path, loader: BulkLoader) -> dict:
        """Process a company data file."""
        parser = CompanyDataParser(file_path)
        total_stats = {'inserted': 0, 'updated': 0, 'skipped': 0}

        for chunk in parser.parse_chunks():
            stats = loader.load_companies(chunk)
            total_stats['inserted'] += stats.get('inserted', 0)
            total_stats['skipped'] += stats.get('skipped', 0)

        return total_stats

    def _process_psc_file(self, file_path: Path, loader: BulkLoader) -> dict:
        """Process a PSC data file."""
        parser = PSCDataParser(file_path)
        total_stats = {'inserted': 0, 'updated': 0}

        for chunk in parser.parse_chunks():
            stats = loader.load_officers(chunk)
            total_stats['inserted'] += stats.get('inserted', 0)

        return total_stats

    def _process_accounts_file(self, file_path: Path, loader: BulkLoader) -> dict:
        """Process an accounts data file."""
        parser = AccountsDataParser(file_path, log_callback=self._log)
        total_stats = {'inserted': 0, 'updated': 0}

        for chunk in parser.parse_chunks():
            stats = loader.load_financials(chunk)
            total_stats['inserted'] += stats.get('inserted', 0)

        return total_stats

    def _create_batch_log(self, batch_id: str, files: list[dict]) -> None:
        """Create ingestion log entry."""
        query = """
            INSERT INTO staging_ingestion_log (
                batch_id, search_name, status, files_total, metadata
            ) VALUES (
                %(batch_id)s, %(search_name)s, 'running', %(files_total)s, %(metadata)s
            )
        """
        from psycopg2.extras import Json
        self.db.execute(query, {
            'batch_id': batch_id,
            'search_name': 'bulk_ingestion',
            'files_total': len(files),
            'metadata': Json({'files': files, 'current_file_index': 0}),
        })

    def _update_batch_status(self, batch_id: str, status: str, error: Optional[str] = None) -> None:
        """Update batch status in database."""
        query = """
            UPDATE staging_ingestion_log
            SET status = %(status)s,
                error_message = %(error)s,
                completed_at = CASE WHEN %(status)s IN ('completed', 'failed') THEN NOW() ELSE completed_at END
            WHERE batch_id = %(batch_id)s
        """
        self.db.execute(query, {
            'batch_id': batch_id,
            'status': status,
            'error': error,
        })

    def _update_batch_progress(self, batch_id: str, files_completed: int) -> None:
        """Update batch progress in database."""
        query = """
            UPDATE staging_ingestion_log
            SET files_completed = %(files_completed)s
            WHERE batch_id = %(batch_id)s
        """
        self.db.execute(query, {
            'batch_id': batch_id,
            'files_completed': files_completed,
        })

    def _save_checkpoint(self, batch_id: str, files: list[dict], current_index: int) -> None:
        """Save checkpoint for resume."""
        query = """
            UPDATE staging_ingestion_log
            SET metadata = %(metadata)s,
                status = 'paused'
            WHERE batch_id = %(batch_id)s
        """
        from psycopg2.extras import Json
        self.db.execute(query, {
            'batch_id': batch_id,
            'metadata': Json({
                'files': files,
                'current_file_index': current_index,
            }),
        })

    def _get_last_paused_batch(self) -> Optional[dict]:
        """Get the last paused batch from database."""
        query = """
            SELECT batch_id, metadata
            FROM staging_ingestion_log
            WHERE status = 'paused'
            ORDER BY started_at DESC
            LIMIT 1
        """
        result = self.db.execute(query, fetch=True)
        return result[0] if result else None

    def _log(self, message: str) -> None:
        """Send log message to callback."""
        timestamp = datetime.now().strftime('%H:%M:%S')
        self.log_callback(f"[{timestamp}] {message}")

    def _notify_progress(self) -> None:
        """Send progress update to callback."""
        if self.progress_callback and self._progress:
            self.progress_callback(self._progress)


# Module-level worker instance
_worker: Optional[IngestionWorker] = None


def get_ingestion_worker(
    log_callback: Optional[Callable[[str], None]] = None,
    progress_callback: Optional[Callable[[IngestionProgress], None]] = None
) -> IngestionWorker:
    """Get or create the ingestion worker instance."""
    global _worker
    if _worker is None:
        _worker = IngestionWorker(log_callback, progress_callback)
    return _worker
