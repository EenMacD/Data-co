#!/bin/bash

# UI Component Runner
# This script runs the Next.js Frontend.

set -e  # Exit on error

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "============================================"
echo "UI Component Startup"
echo "============================================"
echo ""

# 1. Check Node/npm Installation
if ! command -v npm &> /dev/null; then
    echo "❌ npm is not installed. Please install Node.js."
    exit 1
fi

# 2. Install Dependencies
echo "📥 Installing npm dependencies..."
cd "$SCRIPT_DIR"
npm install

# 3. Run the UI
echo ""
echo "============================================"
echo "🚀 Starting Next.js Dev Server..."
echo "============================================"
echo ""
echo "   URL: http://localhost:3000"
echo "   Press Ctrl+C to stop"
echo ""

npm run dev
