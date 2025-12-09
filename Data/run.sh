#!/bin/bash

# Master Run Script for Data Component
# Sets up the environment and launches the Data UI

set -e

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "Using Data directory: $SCRIPT_DIR"

# 1. Check Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 is not installed."
    exit 1
fi

# 2. Setup Virtual Environment if missing
if [ ! -f "bin/activate" ] && [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv .
fi

# Support both 'bin/activate' (Linux/Mac) and standard 'venv/bin/activate' patterns just in case
if [ -f "bin/activate" ]; then
    source bin/activate
elif [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
else
    echo "❌ Could not find virtual environment activation script."
    exit 1
fi

# 3. Install/Upgrade Pip
echo "📥 Checking dependencies..."
pip install --upgrade pip -q

# 4. Run the Bulk Ingestion UI
# We delegate to the specific component script which handles its own specific requirements
if [ -f "bulk_ingestion/run.sh" ]; then
    chmod +x bulk_ingestion/run.sh
    exec ./bulk_ingestion/run.sh
else
    echo "❌ bulk_ingestion/run.sh not found!"
    exit 1
fi
