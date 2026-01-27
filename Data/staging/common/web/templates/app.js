let eventSource = null;
let availableFiles = { company: [], psc: [], accounts: [] };
let selectedFiles = [];

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    refreshStatus();
    connectEventSource();

    // Set default date range (last 3 months)
    const endDate = new Date();
    const startDate = new Date();
    startDate.setMonth(startDate.getMonth() - 3);

    document.getElementById('endDate').valueAsDate = endDate;
    document.getElementById('startDate').valueAsDate = startDate;
});

// Discover Files
document.getElementById('discoverBtn').addEventListener('click', async () => {
    const startDate = document.getElementById('startDate').value;
    const endDate = document.getElementById('endDate').value;

    if (!startDate || !endDate) {
        addLog('[UI] Please select both start and end dates', 'error');
        return;
    }

    addLog('[UI] Discovering available files...', 'info');
    document.getElementById('discoverBtn').disabled = true;
    document.getElementById('discoverBtn').textContent = 'Discovering...';

    // Show loading spinners
    document.getElementById('companyFileList').innerHTML = '<div class="loading-spinner"></div>';
    document.getElementById('pscFileList').innerHTML = '<div class="loading-spinner"></div>';
    document.getElementById('accountsFileList').innerHTML = '<div class="loading-spinner"></div>';
    document.getElementById('fileSelectionCard').style.display = 'block';

    try {
        const response = await fetch('/api/discover-files', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ start_date: startDate, end_date: endDate })
        });

        const result = await response.json();

        if (result.success) {
            availableFiles = result.files;
            renderFileSelections();
            addLog(`[UI] Found ${result.files.company.length} company, ${result.files.psc.length} PSC, ${result.files.accounts.length} accounts files`, 'success');
        } else {
            addLog('[UI] Error discovering files: ' + result.error, 'error');
            document.getElementById('fileSelectionCard').style.display = 'none';
        }
    } catch (error) {
        addLog('[UI] Error: ' + error.message, 'error');
        document.getElementById('fileSelectionCard').style.display = 'none';
    } finally {
        document.getElementById('discoverBtn').disabled = false;
        document.getElementById('discoverBtn').textContent = 'Discover Files';
    }
});

// Render file selections
function renderFileSelections() {
    renderFileList('company', availableFiles.company, 'companyFileList', 'badge-company');
    renderFileList('psc', availableFiles.psc, 'pscFileList', 'badge-psc');
    renderFileList('accounts', availableFiles.accounts, 'accountsFileList', 'badge-accounts');
}

function renderFileList(type, files, containerId, badgeClass) {
    const container = document.getElementById(containerId);

    if (files.length === 0) {
        container.innerHTML = '<div class="empty-state">No files found for this date range</div>';
        return;
    }

    let html = '';
    files.forEach((file, idx) => {
        const fileId = `${type}_${idx}`;
        html += `
            <div class="file-item">
                <input type="checkbox" id="${fileId}" data-type="${type}" data-url="${file.url}" data-filename="${file.filename}" data-date="${file.date}">
                <label for="${fileId}">${file.filename}</label>
            </div>
        `;
    });
    container.innerHTML = html;
}

// Select all checkboxes
document.getElementById('selectAllCompany').addEventListener('change', (e) => {
    document.querySelectorAll('#companyFileList input[type="checkbox"]').forEach(cb => cb.checked = e.target.checked);
});

document.getElementById('selectAllPSC').addEventListener('change', (e) => {
    document.querySelectorAll('#pscFileList input[type="checkbox"]').forEach(cb => cb.checked = e.target.checked);
});

document.getElementById('selectAllAccounts').addEventListener('change', (e) => {
    document.querySelectorAll('#accountsFileList input[type="checkbox"]').forEach(cb => cb.checked = e.target.checked);
});

// Add to ingestion list
document.getElementById('addToListBtn').addEventListener('click', () => {
    const checkboxes = document.querySelectorAll('.file-list input[type="checkbox"]:checked');
    let addedCount = 0;

    checkboxes.forEach(checkbox => {
        const file = {
            product: checkbox.dataset.type,  // Backend expects 'product' key
            url: checkbox.dataset.url,
            filename: checkbox.dataset.filename,
            date: checkbox.dataset.date
        };

        // Check if already in list
        const exists = selectedFiles.some(f => f.url === file.url);
        if (!exists) {
            selectedFiles.push(file);
            addedCount++;
        }
    });

    if (addedCount > 0) {
        renderSelectedFiles();
        addLog(`[UI] Added ${addedCount} file(s) to ingestion list`, 'success');
        document.getElementById('selectedFilesCard').style.display = 'block';
    } else {
        addLog('[UI] No new files to add', 'info');
    }
});

// Render selected files table
function renderSelectedFiles() {
    const tbody = document.getElementById('selectedFilesBody');

    if (selectedFiles.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4" style="text-align: center; padding: 20px; color: #7f8c8d;">No files selected</td></tr>';
        return;
    }

    let html = '';
    selectedFiles.forEach((file, idx) => {
        const badgeClass = file.product === 'company' ? 'badge-company' :
                           file.product === 'psc' ? 'badge-psc' : 'badge-accounts';
        html += `
            <tr>
                <td><span class="file-type-badge ${badgeClass}">${file.product.toUpperCase()}</span></td>
                <td>${file.filename}</td>
                <td>${file.date}</td>
                <td><button class="btn-small btn-danger" onclick="removeFile(${idx})">Remove</button></td>
            </tr>
        `;
    });
    tbody.innerHTML = html;
}

// Remove file from list
function removeFile(index) {
    selectedFiles.splice(index, 1);
    renderSelectedFiles();
    if (selectedFiles.length === 0) {
        document.getElementById('selectedFilesCard').style.display = 'none';
    }
    addLog('[UI] File removed from ingestion list', 'info');
}

// Clear list
document.getElementById('clearListBtn').addEventListener('click', () => {
    if (confirm('Clear all files from the ingestion list?')) {
        selectedFiles = [];
        renderSelectedFiles();
        document.getElementById('selectedFilesCard').style.display = 'none';
        addLog('[UI] Ingestion list cleared', 'info');
    }
});

// Start Ingestion - Show Modal
document.getElementById('startIngestionBtn').addEventListener('click', () => {
    if (selectedFiles.length === 0) {
        addLog('[UI] No files in ingestion list', 'error');
        return;
    }

    // Show modal with file count
    document.getElementById('modalFileCount').textContent = `${selectedFiles.length} file(s) will be processed`;
    document.getElementById('startIngestionModal').style.display = 'block';
});

// Modal functions for Start Ingestion
function closeStartModal() {
    document.getElementById('startIngestionModal').style.display = 'none';
}

async function confirmStartIngestion() {
    closeStartModal();

    try {
        const response = await fetch('/api/ingestion/start', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ files: selectedFiles })
        });

        const result = await response.json();

        if (result.success) {
            addLog(`[UI] Started ingestion of ${selectedFiles.length} file(s)`, 'success');
            document.getElementById('statusBadge').className = 'status-badge status-running';
            document.getElementById('statusBadge').textContent = 'Running';
            document.getElementById('startIngestionBtn').style.display = 'none';
            document.getElementById('stopIngestionBtn').style.display = 'inline-block';
            document.getElementById('progressContainer').style.display = 'block';
        } else {
            addLog('[UI] Error starting ingestion: ' + result.error, 'error');
        }
    } catch (error) {
        addLog('[UI] Error: ' + error.message, 'error');
    }
}

// Stop Ingestion - Show Modal
document.getElementById('stopIngestionBtn').addEventListener('click', () => {
    document.getElementById('stopIngestionModal').style.display = 'block';
});

// Modal functions for Stop Ingestion
function closeStopModal() {
    document.getElementById('stopIngestionModal').style.display = 'none';
}

async function confirmStopIngestion() {
    closeStopModal();

    try {
        const response = await fetch('/api/ingestion/stop', { method: 'POST' });
        const result = await response.json();

        if (result.success) {
            addLog('[UI] Stopping ingestion after current file...', 'info');
            document.getElementById('statusBadge').className = 'status-badge status-stopped';
            document.getElementById('statusBadge').textContent = 'Stopping';
        } else {
            addLog('[UI] Error stopping ingestion: ' + result.error, 'error');
        }
    } catch (error) {
        addLog('[UI] Error: ' + error.message, 'error');
    }
}

// Resume Ingestion
document.getElementById('resumeIngestionBtn').addEventListener('click', async () => {
    try {
        const response = await fetch('/api/ingestion/resume', { method: 'POST' });
        const result = await response.json();

        if (result.success) {
            addLog('[UI] Resuming ingestion...', 'info');
            document.getElementById('statusBadge').className = 'status-badge status-running';
            document.getElementById('statusBadge').textContent = 'Running';
            document.getElementById('resumeIngestionBtn').style.display = 'none';
            document.getElementById('stopIngestionBtn').style.display = 'inline-block';
            document.getElementById('progressContainer').style.display = 'block';
        } else {
            addLog('[UI] Error resuming ingestion: ' + result.error, 'error');
        }
    } catch (error) {
        addLog('[UI] Error: ' + error.message, 'error');
    }
});

// Refresh Status
document.getElementById('refreshStatusBtn').addEventListener('click', refreshStatus);

async function refreshStatus() {
    try {
        const statusResponse = await fetch('/api/status');
        const status = await statusResponse.json();

        document.getElementById('totalCompanies').textContent = status.total_companies || 0;
        document.getElementById('totalOfficers').textContent = status.total_officers || 0;
        document.getElementById('totalFinancials').textContent = status.total_financials || 0;

        const ingestionResponse = await fetch('/api/ingestion/status');
        const ingestionData = await ingestionResponse.json();

        // Extract progress object from response
        const progress = ingestionData.progress;
        const isRunning = ingestionData.is_running;

        if (progress && progress.status === 'running') {
            document.getElementById('statusBadge').className = 'status-badge status-running';
            document.getElementById('statusBadge').textContent = 'Running';
            document.getElementById('statusText').textContent = progress.current_file || 'Processing...';
            document.getElementById('startIngestionBtn').style.display = 'none';
            document.getElementById('stopIngestionBtn').style.display = 'inline-block';
            document.getElementById('resumeIngestionBtn').style.display = 'none';
            document.getElementById('progressContainer').style.display = 'block';
            updateProgressBar(progress.files_completed, progress.files_total, progress.current_file);
        } else if (progress && progress.status === 'stopped') {
            document.getElementById('statusBadge').className = 'status-badge status-stopped';
            document.getElementById('statusBadge').textContent = 'Stopped';
            document.getElementById('statusText').textContent = `Stopped at ${progress.files_completed}/${progress.files_total} files`;
            document.getElementById('stopIngestionBtn').style.display = 'none';
            document.getElementById('resumeIngestionBtn').style.display = 'inline-block';
            document.getElementById('progressContainer').style.display = 'block';
            updateProgressBar(progress.files_completed, progress.files_total, progress.current_file);
        } else if (progress && progress.status === 'completed') {
            document.getElementById('statusBadge').className = 'status-badge status-completed';
            document.getElementById('statusBadge').textContent = 'Completed';
            document.getElementById('statusText').textContent = `Completed ${progress.files_completed} files`;
            document.getElementById('startIngestionBtn').style.display = 'inline-block';
            document.getElementById('stopIngestionBtn').style.display = 'none';
            document.getElementById('resumeIngestionBtn').style.display = 'none';
            document.getElementById('progressContainer').style.display = 'block';
            updateProgressBar(progress.files_completed, progress.files_total, '');
        } else if (progress && progress.status === 'failed') {
            document.getElementById('statusBadge').className = 'status-badge status-failed';
            document.getElementById('statusBadge').textContent = 'Failed';
            document.getElementById('statusText').textContent = progress.error || 'Error occurred';
            document.getElementById('stopIngestionBtn').style.display = 'none';
            document.getElementById('resumeIngestionBtn').style.display = 'inline-block';
        } else {
            document.getElementById('statusBadge').className = 'status-badge status-idle';
            document.getElementById('statusBadge').textContent = 'Idle';
            document.getElementById('statusText').textContent = 'Ready to start';
            document.getElementById('startIngestionBtn').style.display = 'inline-block';
            document.getElementById('stopIngestionBtn').style.display = 'none';
            document.getElementById('resumeIngestionBtn').style.display = 'none';
            document.getElementById('progressContainer').style.display = 'none';
        }
    } catch (error) {
        console.error('Error refreshing status:', error);
    }
}

function updateProgressBar(completed, total, currentFile) {
    const percentage = total > 0 ? (completed / total) * 100 : 0;
    document.getElementById('progressBar').style.width = percentage + '%';
    document.getElementById('progressBar').textContent = Math.round(percentage) + '%';
    document.getElementById('progressText').textContent = `${completed} of ${total} files completed`;
    document.getElementById('currentFile').textContent = currentFile || '';
}

// Connect to SSE logs
function connectEventSource() {
    eventSource = new EventSource('/api/logs');

    eventSource.onmessage = (event) => {
        if (event.data.trim()) {
            try {
                const data = JSON.parse(event.data);
                if (data.message) {
                    addLog(data.message);
                }
                // Ignore heartbeat messages
            } catch (e) {
                // If not JSON, just log the raw data
                addLog(event.data);
            }
        }
    };

    eventSource.onerror = (error) => {
        console.error('EventSource error:', error);
        setTimeout(() => connectEventSource(), 5000);
    };
}

function addLog(message, type = 'info') {
    const logsDiv = document.getElementById('logs');
    const logLine = document.createElement('div');
    logLine.className = 'log-line';

    const timestamp = new Date().toLocaleTimeString();
    logLine.textContent = `[${timestamp}] ${message}`;

    if (type === 'error') {
        logLine.style.color = '#e74c3c';
    } else if (type === 'success') {
        logLine.style.color = '#27ae60';
    } else if (type === 'info') {
        logLine.style.color = '#3498db';
    }

    logsDiv.appendChild(logLine);
    logsDiv.scrollTop = logsDiv.scrollHeight;
}

// Close modals when clicking outside
window.onclick = function(event) {
    const startModal = document.getElementById('startIngestionModal');
    const stopModal = document.getElementById('stopIngestionModal');

    if (event.target === startModal) {
        closeStartModal();
    } else if (event.target === stopModal) {
        closeStopModal();
    }
}

// Auto-refresh status every 5 seconds
setInterval(refreshStatus, 5000);
