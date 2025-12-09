"""
Simple web UI for configuring and running data scraping workflows.
"""
from __future__ import annotations

import os
import sys
import subprocess
import threading
import queue
from pathlib import Path
from datetime import datetime

from flask import Flask, render_template, request, jsonify, Response
import yaml

# Add parent directory to path
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from ingestion_config.loader import load_config

app = Flask(__name__)

# Global queue for streaming logs
log_queue = queue.Queue()
scraping_process = None
scraping_thread = None


@app.route('/')
def index():
    """Main UI page."""
    # Load current config
    config = load_config()
    return render_template('index.html', config=config)


@app.route('/api/config', methods=['GET'])
def get_config():
    """Get current configuration."""
    config = load_config()
    return jsonify(config)


@app.route('/api/config', methods=['POST'])
def save_config():
    """Save configuration to filters.yaml."""
    try:
        data = request.json

        # Build config structure
        config = {
            'search_criteria': {
                'name': data.get('search_name', 'default_search'),
                'selection': {}
            },
            'technical_config': {
                'bulk_download': {
                    'url_template': 'http://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-{date}.zip',
                    'date_format': '%Y-%m-%d',
                    'date_override': data.get('date_override', datetime.now().strftime('%Y-%m-%d'))
                },
                'api_enrichment': {
                    'enabled': data.get('api_enabled', True),
                    'incremental_mode': data.get('incremental_mode', False),
                    'endpoints': data.get('endpoints', ['officers', 'persons-with-significant-control']),
                    'request_rate_per_minute': int(data.get('rate_limit', 120))
                },
                'financials_scan': {
                    'enabled': data.get('financials_enabled', False),
                    'window_years': int(data.get('window_years', 5)),
                    'window_days': int(data.get('window_days', 365)),
                    'download_documents': data.get('download_documents', True),
                    'merge_enriched': data.get('merge_enriched', True),
                    'parse_ixbrl': data.get('parse_ixbrl', True),
                    'parse_xbrl': data.get('parse_xbrl', True),
                    'output_naming': {
                        'folder_template': '{company_name}_{company_number}',
                        'folder_by': 'company_name',
                        'aggregated_template': 'Historical-financial-performance-{company_name}.json',
                        'yearly_template': '{year}, {company_name}.json'
                    },
                    'cleanup': {
                        'remove_documents': data.get('cleanup_documents', True),
                        'remove_financials_dir': data.get('cleanup_financials', True),
                        'remove_raw_dir': data.get('cleanup_raw', True)
                    }
                },
                'storage': {
                    'base_path': "project_data/{name}"
                }
            }
        }

        # Add selection criteria
        selection = config['search_criteria']['selection']

        if data.get('locality'):
            selection['locality'] = data['locality']

        if data.get('company_status'):
            selection['company_status'] = data['company_status']

        if data.get('sic_codes'):
            sic_list = [s.strip() for s in data['sic_codes'].split(',') if s.strip()]
            if sic_list:
                selection['industry_codes'] = {'include': sic_list}

        if data.get('limit'):
            selection['limit'] = int(data['limit'])

        # Save to filters.yaml
        config_path = ROOT / 'config' / 'filters.yaml'
        with open(config_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)

        return jsonify({'success': True, 'message': 'Configuration saved successfully'})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/preview', methods=['POST'])
def preview_results():
    """Preview companies and sample data - doesn't affect database or save config."""
    try:
        from preview_helper import preview_filter_results

        data = request.json
        api_key = os.getenv("COMPANIES_HOUSE_API_KEY")

        # Build filters from request
        filters = {
            'locality': data.get('locality', ''),
            'company_status': data.get('company_status', ''),
            'sic_codes': data.get('sic_codes', ''),
            'limit': data.get('limit') if data.get('limit') else None
        }

        result = preview_filter_results(
            filters=filters,
            api_key=api_key,
            fetch_sample_data=data.get('fetch_sample_data', True)
        )

        return jsonify(result)

    except Exception as e:
        import traceback
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500


@app.route('/api/start', methods=['POST'])
def start_scraping():
    """Start the scraping process."""
    global scraping_thread

    if scraping_thread and scraping_thread.is_alive():
        return jsonify({'success': False, 'error': 'Scraping is already running'}), 400

    # Clear log queue
    while not log_queue.empty():
        log_queue.get()

    # Start scraping in background thread
    scraping_thread = threading.Thread(target=run_scraping_workflow)
    scraping_thread.daemon = True
    scraping_thread.start()

    return jsonify({'success': True, 'message': 'Scraping started'})


@app.route('/api/stop', methods=['POST'])
def stop_scraping():
    """Stop the currently running scraping process."""
    global scraping_process, scraping_thread

    if not scraping_thread or not scraping_thread.is_alive():
        return jsonify({'success': False, 'error': 'No scraping process is running'}), 400

    # Terminate the subprocess
    if scraping_process:
        scraping_process.terminate()
        scraping_process.wait(timeout=5)
        scraping_process = None

    log_queue.put('[UI] Scraping stopped by user')
    return jsonify({'success': True, 'message': 'Scraping stopped'})


@app.route('/api/logs')
def stream_logs():
    """Stream logs to the client."""
    def generate():
        while True:
            try:
                log = log_queue.get(timeout=1)
                yield f"data: {log}\n\n"
            except queue.Empty:
                yield f"data: \n\n"  # Keep connection alive

    return Response(generate(), mimetype='text/event-stream')


@app.route('/api/status', methods=['GET'])
def get_status():
    """Get scraping status."""
    global scraping_thread

    is_running = scraping_thread and scraping_thread.is_alive()

    # Get database stats if available
    try:
        from database.connection import get_staging_db
        db = get_staging_db()

        # Get latest batch info
        batch_query = """
            SELECT batch_id, search_name, started_at, completed_at,
                   companies_count, status, error_message
            FROM staging_ingestion_log
            ORDER BY started_at DESC
            LIMIT 1
        """
        batch_info = db.execute(batch_query, fetch=True)

        # Get total counts
        counts_query = """
            SELECT
                (SELECT COUNT(*) FROM staging_companies) as total_companies,
                (SELECT COUNT(*) FROM staging_officers) as total_officers
        """
        counts = db.execute(counts_query, fetch=True)

        return jsonify({
            'is_running': is_running,
            'latest_batch': batch_info[0] if batch_info else None,
            'total_companies': counts[0]['total_companies'] if counts else 0,
            'total_officers': counts[0]['total_officers'] if counts else 0
        })

    except Exception as e:
        return jsonify({
            'is_running': is_running,
            'error': str(e)
        })


@app.route('/api/duplicates', methods=['GET'])
def get_duplicates():
    """Find duplicate companies in staging database."""
    try:
        from database.connection import get_staging_db
        db = get_staging_db()

        # Find duplicates by company_number
        query = """
            WITH duplicate_companies AS (
                SELECT company_number
                FROM staging_companies
                GROUP BY company_number
                HAVING COUNT(*) > 1
            )
            SELECT
                sc.id,
                sc.batch_id,
                sc.company_number,
                sc.company_name,
                sc.ingested_at,
                (SELECT COUNT(*) FROM staging_officers WHERE staging_company_id = sc.id) > 0 as has_officers,
                (SELECT COUNT(*) FROM staging_financials WHERE staging_company_id = sc.id) > 0 as has_financials
            FROM staging_companies sc
            INNER JOIN duplicate_companies dc ON sc.company_number = dc.company_number
            ORDER BY sc.company_number, sc.ingested_at DESC
        """

        results = db.execute(query, fetch=True)

        # Group by company_number
        duplicates_map = {}
        for row in results:
            company_number = row['company_number']
            if company_number not in duplicates_map:
                duplicates_map[company_number] = {
                    'company_number': company_number,
                    'company_name': row['company_name'],
                    'records': []
                }

            duplicates_map[company_number]['records'].append({
                'id': row['id'],
                'batch_id': row['batch_id'],
                'ingested_at': row['ingested_at'].isoformat() if row['ingested_at'] else None,
                'has_officers': row['has_officers'],
                'has_financials': row['has_financials']
            })

        duplicates = list(duplicates_map.values())

        return jsonify({
            'success': True,
            'duplicates': duplicates,
            'total_groups': len(duplicates)
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/duplicates/delete', methods=['POST'])
def delete_duplicates():
    """Delete selected duplicate records."""
    try:
        from database.connection import get_staging_db
        db = get_staging_db()

        data = request.json
        record_ids = data.get('record_ids', [])

        if not record_ids:
            return jsonify({'success': False, 'error': 'No records selected'}), 400

        # Delete companies (cascade will delete related officers/financials)
        query = """
            DELETE FROM staging_companies
            WHERE id = ANY(%s)
        """

        with db.get_cursor(dict_cursor=False) as cur:
            cur.execute(query, (record_ids,))
            deleted_count = cur.rowcount

        return jsonify({
            'success': True,
            'deleted_count': deleted_count
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


def run_scraping_workflow():
    """Run the scraping workflow and stream output."""
    global scraping_process

    try:
        log_queue.put('[UI] Starting scraping workflow...')

        # Run the API enrichment script
        script_path = ROOT / 'Data-injestion-workflows' / 'Api-request-workflow' / 'api-main-db.py'

        scraping_process = subprocess.Popen(
            [sys.executable, str(script_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )

        # Stream output
        for line in iter(scraping_process.stdout.readline, ''):
            if line:
                log_queue.put(line.rstrip())

        scraping_process.wait()

        if scraping_process.returncode == 0:
            log_queue.put('[UI] Scraping completed successfully!')
        elif scraping_process.returncode == -15:  # SIGTERM
            log_queue.put('[UI] Scraping was stopped')
        else:
            log_queue.put(f'[UI] Scraping failed with exit code {scraping_process.returncode}')

    except Exception as e:
        log_queue.put(f'[UI] Error: {str(e)}')
    finally:
        scraping_process = None


if __name__ == '__main__':
    import socket

    # Find an available port
    def find_free_port(start_port=5000, max_attempts=10):
        for port in range(start_port, start_port + max_attempts):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.bind(('', port))
                    return port
                except OSError:
                    continue
        return None

    port = find_free_port()
    if port is None:
        print('ERROR: Could not find an available port between 5000-5010')
        sys.exit(1)

    print('='*50)
    print('Starting Data Scraping UI...')
    print(f'Open http://localhost:{port} in your browser')
    print('='*50)
    app.run(debug=True, host='0.0.0.0', port=port)
