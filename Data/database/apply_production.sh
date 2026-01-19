#!/bin/bash
set -e

# Load environment variables if not already set (e.g. if run standalone)
if [ -z "$PRODUCTION_DB_HOST" ] && [ -f ../.env ]; then
    export $(grep -v '^#' ../.env | xargs)
fi

DB_NAME=$PRODUCTION_DB_NAME
DB_USER=$PRODUCTION_DB_USER
DB_HOST=${PRODUCTION_DB_HOST:-localhost}
DB_PORT=${PRODUCTION_DB_PORT:-5432}
DB_PASS=$PRODUCTION_DB_PASSWORD

SCRIPT_DIR=$(dirname "$0")
SCHEMA_DIR="$SCRIPT_DIR/production"

echo "Applying Production Schema from $SCHEMA_DIR..."

for file in "$SCHEMA_DIR"/*.sql; do
    echo "  Executing $(basename "$file")..."
    PGPASSWORD=$DB_PASS psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$file" -q
done

echo "Production schema applied successfully."
