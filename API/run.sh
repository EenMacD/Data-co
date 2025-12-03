#!/bin/bash

# API Component Runner
# This script runs the Go API server.

set -e  # Exit on error

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "============================================"
echo "API Component Startup"
echo "============================================"
echo ""

# 1. Check Go Installation
if ! command -v go &> /dev/null; then
    echo "❌ Go is not installed. Please install Go v1.21+."
    exit 1
fi

# 2. Install Dependencies
echo "📥 Downloading Go modules..."
cd "$SCRIPT_DIR"
go mod download

# 3. Environment Variables
# Check for .env in parent directory (root of data-co) or current directory
if [ -f "$SCRIPT_DIR/../.env" ]; then
    echo "✅ Loading environment variables from ../.env"
    export $(grep -v '^#' "$SCRIPT_DIR/../.env" | xargs)
elif [ -f "$SCRIPT_DIR/.env" ]; then
    echo "✅ Loading environment variables from .env"
    export $(grep -v '^#' "$SCRIPT_DIR/.env" | xargs)
else
    echo "⚠️  WARNING: .env file not found. API might fail to connect to DB."
fi

# 4. Run the API
echo ""
echo "============================================"
echo "🚀 Starting API Server..."
echo "============================================"
echo ""
echo "   URL: http://localhost:8080"
echo "   Press Ctrl+C to stop"
echo ""

go run main.go
