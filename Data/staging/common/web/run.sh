#!/bin/bash

# Data Component Runner (Bulk Ingestion UI)
# Just run this script and start downloading files!

set -e  # Exit on error

# Get the directory where this script is located
# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# SCRIPT_DIR is .../Data/staging/common/web
# DATA_DIR is .../Data
DATA_DIR="$(dirname "$(dirname "$(dirname "$SCRIPT_DIR")")")"
PROJECT_ROOT="$(dirname "$DATA_DIR")"

echo "============================================"
echo "Data Ingestion UI"
echo "============================================"
echo ""

# 1. Check Python Installation
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 is not installed. Please install Python 3.8+."
    exit 1
fi

# 2. Activate Virtual Environment
if [ ! -f "$DATA_DIR/bin/activate" ]; then
    echo "❌ Virtual environment not found at $DATA_DIR/bin/activate"
    echo "   Please create it first by running from Data directory:"
    echo "   cd Data && python3 -m venv ."
    exit 1
fi

echo "📦 Activating virtual environment..."
source "$DATA_DIR/bin/activate"

# 3. Auto-install Dependencies
echo "📥 Installing/updating dependencies..."
cd "$SCRIPT_DIR"
pip3 install --upgrade pip -q
pip3 install -r ../../../../requirements.txt -q
echo "✅ Dependencies ready"

# 4. Load Environment Variables
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
else
    echo "⚠️  WARNING: .env file not found in project root"
    echo "    Copy .env.example to .env and configure it"
    exit 1
fi

# 5. Auto-start Database
echo "🔍 Checking database..."
if ! docker ps | grep -q "data-co-db-staging"; then
    echo "   Starting database container..."
    cd "$PROJECT_ROOT"

    if docker-compose up db-staging -d; then
        echo "   Waiting for database to initialize..."
        sleep 5

        # Verify it actually started
        if ! docker ps | grep -q "data-co-db-staging"; then
            echo "❌ Database failed to start. Check Docker Desktop is running."
            exit 1
        fi
        echo "✅ Database started successfully"
    else
        echo "❌ Failed to start database. Make sure Docker Desktop is running."
        exit 1
    fi
else
    echo "✅ Database already running"
fi

# 6. Auto-apply Migrations (only if needed)
# 6. Database Check
# We assume database is set up via setup_databases.sh
echo "✅ Database ready"

# 7. Run the Flask UI
echo ""
echo "============================================"
echo "🚀 Starting UI..."
echo "============================================"
echo ""
echo "   🌐 Open: http://localhost:${DATA_UI_PORT}"
echo "   📊 Ready to download Companies House data"
echo "   ⏹️  Press Ctrl+C to stop"
echo ""

cd "$DATA_DIR"
export FLASK_APP=staging/common/web/app.py
export FLASK_DEBUG=1

python3 staging/common/web/app.py
