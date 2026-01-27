#!/bin/bash

# Setup script for Companies House two-database architecture
# This creates both staging and production PostgreSQL databases

set -e  # Exit on error

echo "╔════════════════════════════════════════════════════════╗"
echo "║  Companies House Database Setup                        ║"
echo "║  Two-database architecture (staging + production)      ║"
echo "╚════════════════════════════════════════════════════════╝"
echo ""

# Load environment variables (skip if already set by docker-compose)
if [ -z "$STAGING_DB_HOST" ]; then
    if [ -f ../.env ]; then
        echo "✓ Found .env file in project root"
        # Robustly load .env across OSes (handles \r, comments, and empty lines)
        while IFS= read -r line || [ -n "$line" ]; do
            # Strip carriage returns (Windows) and inline comments
            clean_line=$(echo "$line" | tr -d '\r' | sed 's/#.*//' | xargs)
            [ -z "$clean_line" ] && continue
            export "$clean_line"
        done < ../.env
    else
        echo "✗ .env file not found in project root"
        echo "  Please copy .env.example to .env and configure it first"
        echo ""
        echo "  cp .env.example .env"
        echo "  nano .env"
        echo ""
        exit 1
    fi
else
    echo "✓ Using environment variables from Docker"
fi

# Check if PostgreSQL is installed
if ! command -v psql &> /dev/null; then
    echo "✗ PostgreSQL not found"
    echo "  Please install PostgreSQL first:"
    echo ""
    echo "  macOS:   brew install postgresql"
    echo "  Ubuntu:  sudo apt-get install postgresql"
    echo "  Windows: Download from https://www.postgresql.org/download/"
    echo ""
    exit 1
fi

echo "✓ PostgreSQL is installed"
echo ""

# Function to create database if it doesn't exist
create_database() {
    local db_name=$1
    local db_user=$2
    local db_host=$3
    local db_port=$4
    local db_password=$5
    local db_admin_user=$6

    echo "→ Checking if database '$db_name' exists on $db_host:$db_port..."

    if PGPASSWORD=$db_password psql -h "$db_host" -p "$db_port" -U "$db_admin_user" -lqt | cut -d \| -f 1 | grep -qw "$db_name"; then
        echo "  ℹ Database '$db_name' already exists"
        read -p "  Do you want to drop and recreate it? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo "  Disconnecting active sessions..."
            PGPASSWORD=$db_password psql -h "$db_host" -p "$db_port" -U "$db_admin_user" -d "postgres" -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$db_name' AND pid <> pg_backend_pid();" > /dev/null

            echo "  Dropping database '$db_name'..."
            PGPASSWORD=$db_password dropdb -h "$db_host" -p "$db_port" -U "$db_admin_user" "$db_name" || echo "  (ignoring error)"
            echo "  Creating database '$db_name'..."
            PGPASSWORD=$db_password createdb -h "$db_host" -p "$db_port" -U "$db_admin_user" "$db_name" -O "$db_user"
        else
            echo "  Keeping existing database"
            return
        fi
    else
        echo "  Creating database '$db_name'..."
        PGPASSWORD=$db_password createdb -h "$db_host" -p "$db_port" -U "$db_admin_user" "$db_name" -O "$db_user"
    fi

    echo "  ✓ Database '$db_name' ready"
}

# Function to apply schema
apply_schema() {
    local db_name=$1
    local schema_file=$2
    local db_host=$3
    local db_port=$4
    local db_user=$5
    local db_password=$6

    echo "→ Applying schema to '$db_name' on $db_host:$db_port..."

    if [ ! -f "$schema_file" ]; then
        echo "  ✗ Schema file not found: $schema_file"
        exit 1
    fi

    PGPASSWORD=$db_password psql -h "$db_host" -p "$db_port" -U "$db_user" -d "$db_name" -f "$schema_file" -q

    if [ $? -eq 0 ]; then
        echo "  ✓ Schema applied successfully"
    else
        echo "  ✗ Failed to apply schema"
        exit 1
    fi
}

# =====================================================
# STAGING DATABASE
# =====================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "STAGING DATABASE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ -z "$STAGING_DB_NAME" ] || [ -z "$STAGING_DB_USER" ]; then
    echo "✗ Missing staging database config in .env"
    echo "  Required: STAGING_DB_NAME, STAGING_DB_USER"
    exit 1
fi

create_database "$STAGING_DB_NAME" "$STAGING_DB_USER" "$STAGING_DB_HOST" "$STAGING_DB_PORT" "$STAGING_DB_PASSWORD" "$STAGING_DB_USER"
bash staging/common/schemas/apply_staging.sh

echo ""

# =====================================================
# PRODUCTION DATABASE
# =====================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "PRODUCTION DATABASE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ -z "$PRODUCTION_DB_NAME" ] || [ -z "$PRODUCTION_DB_USER" ]; then
    echo "✗ Missing production database config in .env"
    echo "  Required: PRODUCTION_DB_NAME, PRODUCTION_DB_USER"
    exit 1
fi

create_database "$PRODUCTION_DB_NAME" "$PRODUCTION_DB_USER" "$PRODUCTION_DB_HOST" "$PRODUCTION_DB_PORT" "$PRODUCTION_DB_PASSWORD" "$PRODUCTION_DB_USER"
bash production/common/schemas/apply_production.sh

echo ""

# =====================================================
# TEST CONNECTIONS
# =====================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "TESTING CONNECTIONS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo "→ Testing Python database connections..."
python staging/common/services/connection.py

echo ""

# =====================================================
# SUMMARY
# =====================================================
echo "╔════════════════════════════════════════════════════════╗"
echo "║  ✓ Setup Complete!                                     ║"
echo "╚════════════════════════════════════════════════════════╝"
echo ""
echo "Your databases are ready:"
echo ""
echo "  STAGING:    $STAGING_DB_NAME"
echo "  PRODUCTION: $PRODUCTION_DB_NAME"
echo ""
echo "Next steps:"
echo "  1. Apply database migrations:"
echo "     Running setup_databases.sh is sufficient for now."
echo ""
echo "  2. Access the Data Ingestion UI:"
echo "     http://localhost:\${DATA_UI_PORT}"
echo ""
echo "  3. Use the UI to:"
echo "     - Select date range → Discover Files"
echo "     - Select files from panels → Add to Ingestion List"
echo "     - Start Ingestion → Monitor progress"
echo ""
echo "  4. Merge data to production:"
echo "     docker exec -it data-co-worker python production/common/services/merge_to_production.py --list"
echo ""
echo "For detailed documentation, see data_ingestion/README.md"
echo ""
