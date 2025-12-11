"""
Flask application for data ingestion UI.

Provides endpoints for:
- File discovery from Companies House
- Ingestion control (start/stop/resume)
- Progress tracking via SSE
- Database status
"""
from __future__ import annotations

import json
import os
import sys
import threading
from datetime import date, datetime
from pathlib import Path
from queue import Queue, Empty

from flask import Flask, render_template, request, jsonify, Response

# Add parent paths for imports
DATA_ROOT = Path(__file__).resolve().parents[2]  # Data/
BULK_INGESTION_ROOT = Path(__file__).resolve().parents[1]  # Data/bulk_ingestion/

if str(DATA_ROOT) not in sys.path:
    sys.path.append(str(DATA_ROOT))
if str(BULK_INGESTION_ROOT) not in sys.path:
    sys.path.append(str(BULK_INGESTION_ROOT))

from database.connection import get_staging_db

# Import our services
from services.file_discovery import FileDiscoveryService, get_file_discovery_service
from services.ingestion_worker import IngestionWorker, IngestionProgress

app = Flask(__name__)

# Global state
log_queue: Queue = Queue()
worker: IngestionWorker | None = None
worker_lock = threading.Lock()


def get_worker() -> IngestionWorker:
    """Get or create the ingestion worker."""
    global worker
    with worker_lock:
        if worker is None:
            worker = IngestionWorker(
                log_callback=lambda msg: log_queue.put(msg),
                progress_callback=None  # Progress via status endpoint
            )
        return worker


# ============= MAIN ROUTES =============

@app.route('/')
def index():
    """Serve the main dashboard."""
    return render_template('index.html')


# ============= FILE DISCOVERY =============

@app.route('/api/discover-files', methods=['POST'])
def discover_files():
    """
    Discover available files for a date range across all product types.

    Request body:
    {
        "start_date": "YYYY-MM-DD",
        "end_date": "YYYY-MM-DD"
    }

    Response:
    {
        "success": true,
        "files": {
            "company": [{"product": "company", "url": "...", "filename": "...", "date": "..."}],
            "psc": [...],
            "accounts": [...]
        }
    }
    """
    data = request.get_json() or {}

    start_date_str = data.get('start_date')
    end_date_str = data.get('end_date')

    if not all([start_date_str, end_date_str]):
        return jsonify({
            'success': False,
            'error': 'Missing required fields: start_date, end_date'
        }), 400

    try:
        start_date = date.fromisoformat(start_date_str)
        end_date = date.fromisoformat(end_date_str)
    except ValueError as e:
        return jsonify({
            'success': False,
            'error': f'Invalid date format: {e}'
        }), 400

    try:
        service = get_file_discovery_service()

        # Discover files for all product types
        all_files = {'company': [], 'psc': [], 'accounts': []}

        for product in ['company', 'psc', 'accounts']:
            files = service.discover_files(
                product=product,
                start_date=start_date,
                end_date=end_date,
                monthly_only=True  # Only first-of-month for PSC and Accounts
            )
            all_files[product] = [f.to_dict() for f in files]

            # Debug logging
            print(f"[DEBUG] {product}: Found {len(files)} files for date range {start_date} to {end_date}")
            if product == 'psc' and len(files) == 0:
                # Check if there are any PSC files at all
                all_psc = service._get_all_files('psc')
                print(f"[DEBUG] Total PSC files available: {len(all_psc)}")
                if all_psc:
                    print(f"[DEBUG] Sample PSC dates: {[f.file_date.isoformat() for f in all_psc[:5]]}")

        return jsonify({
            'success': True,
            'files': all_files
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# ============= INGESTION CONTROL =============

@app.route('/api/ingestion/start', methods=['POST'])
def start_ingestion():
    """
    Start processing selected files.

    Request body:
    {
        "files": [
            {"product": "company", "url": "...", "file_date": "...", ...}
        ]
    }

    Response:
    {
        "success": true,
        "batch_id": "bulk_20240101_123456_abc123"
    }
    """
    data = request.get_json() or {}
    files = data.get('files', [])

    if not files:
        return jsonify({
            'success': False,
            'error': 'No files provided'
        }), 400

    try:
        w = get_worker()

        if w.is_running:
            return jsonify({
                'success': False,
                'error': 'Ingestion is already running'
            }), 400

        batch_id = w.start(files)

        return jsonify({
            'success': True,
            'batch_id': batch_id
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/ingestion/stop', methods=['POST'])
def stop_ingestion():
    """
    Stop the running ingestion (pauses after current file).

    Response:
    {
        "success": true,
        "message": "Stop requested"
    }
    """
    try:
        w = get_worker()

        if not w.is_running:
            return jsonify({
                'success': False,
                'error': 'No ingestion is running'
            }), 400

        w.stop()

        return jsonify({
            'success': True,
            'message': 'Stop requested - will pause after current file completes'
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/ingestion/resume', methods=['POST'])
def resume_ingestion():
    """
    Resume from last paused batch.

    Response:
    {
        "success": true,
        "batch_id": "bulk_..."
    }
    """
    try:
        w = get_worker()

        if w.is_running:
            return jsonify({
                'success': False,
                'error': 'Ingestion is already running'
            }), 400

        batch_id = w.resume()

        if not batch_id:
            return jsonify({
                'success': False,
                'error': 'No paused batch found to resume'
            }), 404

        return jsonify({
            'success': True,
            'batch_id': batch_id
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/ingestion/status', methods=['GET'])
def get_ingestion_status():
    """
    Get detailed ingestion progress.

    Response:
    {
        "is_running": true,
        "progress": {
            "batch_id": "...",
            "status": "running",
            "files_total": 10,
            "files_completed": 3,
            "current_file": "...",
            "overall_progress": 35.5,
            ...
        }
    }
    """
    try:
        w = get_worker()

        progress = w.progress.to_dict() if w.progress else None

        return jsonify({
            'is_running': w.is_running,
            'progress': progress
        })

    except Exception as e:
        return jsonify({
            'is_running': False,
            'progress': None,
            'error': str(e)
        })


# ============= LOGS (SSE) =============

@app.route('/api/logs')
def stream_logs():
    """
    Server-Sent Events endpoint for real-time logs.

    Returns a stream of log messages.
    """
    def generate():
        while True:
            try:
                # Get log message with timeout
                message = log_queue.get(timeout=30)
                yield f"data: {json.dumps({'message': message})}\n\n"
            except Empty:
                # Send heartbeat
                yield f"data: {json.dumps({'heartbeat': True})}\n\n"

    return Response(
        generate(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
        }
    )


# ============= DATABASE STATUS =============

@app.route('/api/status', methods=['GET'])
def get_status():
    """
    Get database statistics.

    Response:
    {
        "total_companies": 1234567,
        "total_officers": 4567890,
        "total_financials": 123456,
        "latest_batch": {...}
    }
    """
    try:
        db = get_staging_db()

        # Get counts
        company_count = db.execute(
            "SELECT COUNT(*) as count FROM staging_companies",
            fetch=True
        )[0]['count']

        officer_count = db.execute(
            "SELECT COUNT(*) as count FROM staging_officers",
            fetch=True
        )[0]['count']

        financial_count = db.execute(
            "SELECT COUNT(*) as count FROM staging_financials",
            fetch=True
        )[0]['count']

        # Get latest batch
        latest_batch = db.execute("""
            SELECT batch_id, search_name, started_at, completed_at,
                   companies_count, status, files_total, files_completed
            FROM staging_ingestion_log
            ORDER BY started_at DESC
            LIMIT 1
        """, fetch=True)

        batch_info = None
        if latest_batch:
            b = latest_batch[0]
            batch_info = {
                'batch_id': b['batch_id'],
                'search_name': b['search_name'],
                'started_at': b['started_at'].isoformat() if b['started_at'] else None,
                'completed_at': b['completed_at'].isoformat() if b['completed_at'] else None,
                'companies_count': b['companies_count'],
                'status': b['status'],
                'files_total': b.get('files_total', 0),
                'files_completed': b.get('files_completed', 0),
            }

        return jsonify({
            'total_companies': company_count,
            'total_officers': officer_count,
            'total_financials': financial_count,
            'latest_batch': batch_info
        })

    except Exception as e:
        return jsonify({
            'total_companies': 0,
            'total_officers': 0,
            'total_financials': 0,
            'latest_batch': None,
            'error': str(e)
        })


# ============= MAIN =============

if __name__ == '__main__':
    # Load port from environment variable
    # In Docker, this is effectively overridden by the port mapping, 
    # but for local dev, we want to respect the .env
    port = int(os.environ.get('DATA_UI_PORT'))
    
    print(f"\n{'='*50}")
    print(f"  Data Ingestion Dashboard")
    print(f"  Running at: http://localhost:{port}")
    print(f"{'='*50}\n")

    app.run(
        host='0.0.0.0',  # Bind to all interfaces for Docker
        port=port,       # Bind to the configured port (e.g., 5001)
        debug=True,
        threaded=True
    )
