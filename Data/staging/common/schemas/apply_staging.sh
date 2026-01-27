#!/bin/bash
set -e

# Load environment variables if not already set (e.g. if run standalone)
if [ -z "$STAGING_DB_HOST" ] && [ -f ../../../../.env ]; then
    export $(grep -v '^#' ../../../../.env | xargs)
fi

DB_NAME=$STAGING_DB_NAME
DB_USER=$STAGING_DB_USER
DB_HOST=${STAGING_DB_HOST:-localhost}
DB_PORT=${STAGING_DB_PORT:-5432}
DB_PASS=$STAGING_DB_PASSWORD

SCRIPT_DIR=$(dirname "$0")
SCHEMA_DIR="."

# Find all SQL files, sort by FILENAME (ignore path), and apply
echo "  Collecting and sorting schema files..."

find "$SCHEMA_DIR"/staging/common/schemas \
     "$SCHEMA_DIR"/staging/tables/companies/schemas \
     "$SCHEMA_DIR"/staging/tables/psc/schemas \
     "$SCHEMA_DIR"/staging/tables/accounts/schemas \
     -name "*.sql" -print0 | \
     perl -0ne 'print "$_"' | \
     xargs -0 -I{} bash -c 'echo "$(basename "{}") {}"' | sort | cut -d ' ' -f2- | \
while read file; do
    [ -e "$file" ] || continue
    echo "    $(basename "$file")"
    PGPASSWORD=$DB_PASS psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$file" -q
done

echo "Staging schema applied successfully."
