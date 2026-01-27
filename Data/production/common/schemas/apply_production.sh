#!/bin/bash
set -e

# Load environment variables if not already set (e.g. if run standalone)
if [ -z "$PRODUCTION_DB_HOST" ] && [ -f ../../../../.env ]; then
    export $(grep -v '^#' ../../../../.env | xargs)
fi

DB_NAME=$PRODUCTION_DB_NAME
DB_USER=$PRODUCTION_DB_USER
DB_HOST=${PRODUCTION_DB_HOST:-localhost}
DB_PORT=${PRODUCTION_DB_PORT:-5432}
DB_PASS=$PRODUCTION_DB_PASSWORD

SCRIPT_DIR=$(dirname "$0")
# Order matters: Common (setup) -> Companies -> PSC -> Accounts
SCHEMA_DIR="."

# Find all SQL files, sort by FILENAME (ignore path), and apply
echo "  Collecting and sorting schema files..."

find "$SCHEMA_DIR"/production/common/schemas \
     "$SCHEMA_DIR"/production/tables/companies/schemas \
     "$SCHEMA_DIR"/production/tables/psc/schemas \
     "$SCHEMA_DIR"/production/tables/accounts/schemas \
     -name "*.sql" -print0 | \
     perl -0ne 'print "$_"' | \
     xargs -0 -I{} bash -c 'echo "$(basename "{}") {}"' | sort | cut -d ' ' -f2- | \
while read file; do
    [ -e "$file" ] || continue
    echo "    $(basename "$file")"
    PGPASSWORD=$DB_PASS psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$file" -q
done

echo "Production schema applied successfully."
