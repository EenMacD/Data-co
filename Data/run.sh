#!/bin/bash

# Data Component Runner
# This script sets up the environment and runs the Data Ingestion UI.

set -e  # Exit on error

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
UI_DIR="$SCRIPT_DIR/ui"

echo "============================================"
echo "Data Component Startup"
echo "============================================"
echo ""

# 1. Virtual Environment Setup
if [ -d "$SCRIPT_DIR/bin" ] && [ -f "$SCRIPT_DIR/bin/activate" ]; then
    # Existing venv structure in Data/bin (common in this project)
    VENV_PATH="$SCRIPT_DIR"
    echo "📦 Activating existing virtual environment in $VENV_PATH"
    source "$VENV_PATH/bin/activate"
elif [ -d "$SCRIPT_DIR/venv" ]; then
    VENV_PATH="$SCRIPT_DIR/venv"
    echo "📦 Activating virtual environment: $VENV_PATH"
    source "$VENV_PATH/bin/activate"
else
    echo "⚠️  No virtual environment found."
    echo "📦 Creating new virtual environment in venv..."
    python3 -m venv venv
    source venv/bin/activate
fi

# 2. Install Dependencies
echo "📥 Checking/Installing dependencies..."
pip install -r "$SCRIPT_DIR/requirements.txt"
pip install -r "$UI_DIR/requirements.txt"

# 3. Environment Variables
if [ -f "$SCRIPT_DIR/.env" ]; then
    echo "✅ Loading environment variables from .env"
    export $(grep -v '^#' "$SCRIPT_DIR/.env" | xargs)
else
    echo "⚠️  WARNING: .env file not found at $SCRIPT_DIR/.env"
    echo "   Please create it with your database credentials."
fi

# 4. Run the UI
echo ""
echo "============================================"
echo "🚀 Starting Data UI Server..."
echo "============================================"
echo ""
echo "   URL: http://localhost:5000"
echo "   Press Ctrl+C to stop"
echo ""

# Run app.py from the UI directory to ensure relative paths work
cd "$UI_DIR"
python3 app.py
