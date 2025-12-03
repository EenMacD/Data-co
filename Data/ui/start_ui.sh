#!/bin/bash

# Data Scraping UI Startup Script
# This script activates the virtual environment and starts the Flask UI

set -e  # Exit on error

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

echo "============================================"
echo "Data Scraping UI Startup"
echo "============================================"
echo ""

# Check if we're already in a virtual environment
if [ -z "$VIRTUAL_ENV" ]; then
    # Look for venv in common locations
    if [ -d "$PROJECT_ROOT/venv" ]; then
        VENV_PATH="$PROJECT_ROOT/venv"
    elif [ -d "$PROJECT_ROOT/../venv" ]; then
        VENV_PATH="$PROJECT_ROOT/../venv"
    elif [ -d "$HOME/.virtualenvs/data-co" ]; then
        VENV_PATH="$HOME/.virtualenvs/data-co"
    else
        echo "❌ Virtual environment not found!"
        echo ""
        echo "Please create a virtual environment first:"
        echo "  python3 -m venv venv"
        echo "  source venv/bin/activate"
        echo "  pip install -r requirements.txt"
        echo ""
        exit 1
    fi

    echo "📦 Activating virtual environment: $VENV_PATH"
    source "$VENV_PATH/bin/activate"
else
    echo "✅ Virtual environment already active: $VIRTUAL_ENV"
fi

# Verify Python 3 is being used
PYTHON_VERSION=$(python3 --version 2>&1)
echo "🐍 Python version: $PYTHON_VERSION"

# Check if .env file exists
if [ ! -f "$PROJECT_ROOT/.env" ]; then
    echo ""
    echo "⚠️  WARNING: .env file not found at $PROJECT_ROOT/.env"
    echo "   Please create it with your database credentials:"
    echo ""
    echo "   STAGING_DB_HOST=localhost"
    echo "   STAGING_DB_PORT=5432"
    echo "   STAGING_DB_NAME=your_database"
    echo "   STAGING_DB_USER=your_username"
    echo "   STAGING_DB_PASSWORD=your_password"
    echo "   COMPANIES_HOUSE_API_KEY=your_api_key"
    echo ""
fi

# Load environment variables from .env
if [ -f "$PROJECT_ROOT/.env" ]; then
    echo "✅ Loading environment variables from .env"
    export $(grep -v '^#' "$PROJECT_ROOT/.env" | xargs)
fi

# Check if Flask is installed
if ! python3 -c "import flask" 2>/dev/null; then
    echo ""
    echo "📥 Flask not found. Installing requirements..."
    pip install -r "$SCRIPT_DIR/requirements.txt"
else
    echo "✅ Flask is installed"
fi

# Check if config file exists
CONFIG_FILE="$PROJECT_ROOT/config/filters.yaml"
if [ ! -f "$CONFIG_FILE" ]; then
    echo "⚠️  Warning: Config file not found at $CONFIG_FILE"
fi

echo ""
echo "============================================"
echo "🚀 Starting UI Server..."
echo "============================================"
echo ""
echo "   URL: http://localhost:5000"
echo ""
echo "   Press Ctrl+C to stop the server"
echo ""

# Change to UI directory and start the server
cd "$SCRIPT_DIR"
python3 app.py
