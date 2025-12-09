#!/bin/bash

# Master Run Script
# Starts the entire project using Docker Compose.
# Each developer's .env file controls their database credentials.

echo "🚀 Starting Project..."

# Get the absolute path of the project root
PROJECT_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$PROJECT_ROOT"

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "❌ No .env file found!"
    echo ""
    echo "   Please create one by running:"
    echo "   cp .env.example .env"
    echo ""
    echo "   Then edit .env with your database credentials."
    exit 1
fi

echo "✅ Found .env file"
echo "   Loading configuration..."

# Load environment variables
set -a
source .env
set +a

# Show configuration
echo ""
echo "📋 Configuration:"
echo "   Host IP:    ${HOST_IP:-localhost}"
echo "   UI:         http://${HOST_IP:-localhost}:${UI_PORT:-3000}"
echo "   API:        http://${HOST_IP:-localhost}:${API_PORT:-8080}/api"
echo "   Data UI:    http://${HOST_IP:-localhost}:${DATA_UI_PORT:-5001}"
echo "   Database:   localhost:${DB_EXTERNAL_PORT:-5433} (${POSTGRES_DB:-staging})"
echo ""

# Handle command line arguments
case "${1:-}" in
    --down|-d)
        echo "🛑 Stopping containers..."
        docker compose down
        echo "✅ Containers stopped."
        exit 0
        ;;
    --restart|-r)
        echo "🔄 Restarting containers..."
        docker compose down
        docker compose up -d --build
        echo "✅ Containers restarted in background."
        exit 0
        ;;
    --logs|-l)
        docker compose logs -f
        exit 0
        ;;
    --data-setup)
        echo "📦 Setting up Data component Python environment..."
        cd "$PROJECT_ROOT/Data"
        if [ ! -d "venv" ]; then
            python3 -m venv venv
            echo "✅ Created virtual environment at Data/venv"
        fi
        source venv/bin/activate
        pip3 install --upgrade pip -q
        pip3 install -r bulk_ingestion/web/requirements.txt -q
        echo "✅ Data component setup complete"
        echo ""
        echo "Next steps:"
        echo "  ./run_project.sh --data-migrate   # Apply database migrations"
        echo "  ./run_project.sh --data-ui        # Run data UI in dev mode"
        exit 0
        ;;
    --data-migrate)
        echo "🗄️  Applying database migrations..."
        # Start database if not running
        if ! docker ps | grep -q "data-co-db-staging"; then
            echo "   Starting database..."
            docker compose up db-staging -d
            sleep 5
        fi
        cd "$PROJECT_ROOT/Data"
        if [ ! -d "venv" ]; then
            echo "❌ Virtual environment not found. Run: ./run_project.sh --data-setup"
            exit 1
        fi
        source venv/bin/activate
        python3 database/apply_migrations.py
        echo "✅ Migrations applied"
        exit 0
        ;;
    --data-ui)
        echo "🌐 Running Data UI (local development mode)..."
        exec "$PROJECT_ROOT/Data/run.sh"
        ;;
    --help|-h)
        echo "Usage: ./run_project.sh [option]"
        echo ""
        echo "Docker Options:"
        echo "  (none)          Start all containers in foreground"
        echo "  -d, --down      Stop all containers"
        echo "  -r, --restart   Rebuild and restart in background"
        echo "  -l, --logs      Follow container logs"
        echo ""
        echo "Data Component Options (local development):"
        echo "  --data-setup    Setup Python venv and install dependencies"
        echo "  --data-migrate  Apply database migrations"
        echo "  --data-ui       Run Flask data UI in development mode"
        echo ""
        echo "  -h, --help      Show this help"
        echo ""
        echo "Examples:"
        echo "  # Full project with Docker"
        echo "  ./run_project.sh"
        echo ""
        echo "  # Data component only (local dev)"
        echo "  ./run_project.sh --data-setup"
        echo "  ./run_project.sh --data-migrate"
        echo "  ./run_project.sh --data-ui"
        exit 0
        ;;
esac

# Start Docker Compose
# Function to open a new terminal window
open_terminal() {
    local cmd="$1"
    local title="$2"
    
    # Escape quotes and backslashes in title and cmd for AppleScript string
    # We need multiple levels of escaping for heredoc + AppleScript
    # However, for simplicity and robustness, we will rely on single-quoted heredoc to prevent shell expansion of backslashes,
    # but we still need to inject variables.
    # Best approach: Use proper escaping for the literal parts.
    
    osascript <<EOF
    tell application "Terminal"
        do script "echo -n -e \"\\\\033]0;$title\\\\007\"; $cmd"
    end tell
EOF
}

# Start Docker Compose
echo "🐳 Starting Databases..."
# Start databases in background first
docker compose up db-staging db-production -d

echo "🚀 Launching Services in separate terminals..."

# 1. API
open_terminal "cd '$PROJECT_ROOT' && docker compose up api" "DataCo API"

# 2. UI
open_terminal "cd '$PROJECT_ROOT' && docker compose up ui" "DataCo UI"

# 3. Data Worker
open_terminal "cd '$PROJECT_ROOT' && docker compose up data-worker" "DataCo Worker"

# 4. Data UI (Local with venv)
# Ensure script is executable
chmod +x "$PROJECT_ROOT/Data/run.sh"
open_terminal "$PROJECT_ROOT/Data/run.sh" "DataCo Data UI"

echo "✅ All services launched in separate windows."
echo "   Run './run_project.sh --down' to stop everything."
