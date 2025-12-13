#!/bin/bash
set -e

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "WARNING: This will delete all data in the databases."
echo "Stopping containers and removing volumes..."
docker compose down -v

echo "Starting containers..."
docker compose up -d

echo "Waiting for PostgreSQL to initialize (10 seconds)..."
sleep 10

echo "Running database setup..."
# Ensure setup_databases.sh is executable
chmod +x Data/setup_databases.sh

# Change to Data directory
cd Data

# Export environment variables from root .env (which is now ../.env)
if [ -f ../.env ]; then
    echo "Loading environment variables from root .env..."
    export $(grep -v '^#' ../.env | xargs)
fi

# Set host variables to localhost since we are running from host
export STAGING_DB_HOST=localhost
export PRODUCTION_DB_HOST=localhost

# Run the setup script
./setup_databases.sh
