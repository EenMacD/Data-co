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
    if [ -f .env ]; then
        echo "✓ Found .env file"
        export $(cat .env | grep -v '^#' | xargs)
    else
        echo "✗ .env file not found"
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

    echo "→ Checking if database '$db_name' exists..."

    if PGPASSWORD=$STAGING_DB_PASSWORD psql -h "$STAGING_DB_HOST" -U "$STAGING_DB_USER" -lqt | cut -d \| -f 1 | grep -qw "$db_name"; then
        echo "  ℹ Database '$db_name' already exists"
        read -p "  Do you want to drop and recreate it? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo "  Dropping database '$db_name'..."
            PGPASSWORD=$STAGING_DB_PASSWORD dropdb -h "$STAGING_DB_HOST" -U "$STAGING_DB_USER" "$db_name" || echo "  (ignoring error)"
            echo "  Creating database '$db_name'..."
            PGPASSWORD=$STAGING_DB_PASSWORD createdb -h "$STAGING_DB_HOST" -U "$STAGING_DB_USER" "$db_name" -O "$db_user"
        else
            echo "  Keeping existing database"
            return
        fi
    else
        echo "  Creating database '$db_name'..."
        PGPASSWORD=$STAGING_DB_PASSWORD createdb -h "$STAGING_DB_HOST" -U "$STAGING_DB_USER" "$db_name" -O "$db_user"
    fi

    echo "  ✓ Database '$db_name' ready"
}

# Function to apply schema
apply_schema() {
    local db_name=$1
    local schema_file=$2

    echo "→ Applying schema to '$db_name'..."

    if [ ! -f "$schema_file" ]; then
        echo "  ✗ Schema file not found: $schema_file"
        exit 1
    fi

    PGPASSWORD=$STAGING_DB_PASSWORD psql -h "$STAGING_DB_HOST" -U "$STAGING_DB_USER" -d "$db_name" -f "$schema_file" -q

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

create_database "$STAGING_DB_NAME" "$STAGING_DB_USER"
apply_schema "$STAGING_DB_NAME" "database/schema_staging.sql"

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

create_database "$PRODUCTION_DB_NAME" "$PRODUCTION_DB_USER"
apply_schema "$PRODUCTION_DB_NAME" "database/schema_production.sql"

echo ""

# =====================================================
# TEST CONNECTIONS
# =====================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "TESTING CONNECTIONS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo "→ Testing Python database connections..."
python database/connection.py

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
echo ""
echo "  1. Run data ingestion to staging:"
echo "     python Data-injestion-workflows/Api-request-workflow/api-main-db.py"
echo ""
echo "  2. List batches available for merging:"
echo "     python database/merge_to_production.py --list"
echo ""
echo "  3. Merge a batch to production:"
echo "     python database/merge_to_production.py --batch-id <batch_id>"
echo ""
echo "For detailed documentation, see database/README.md"
echo ""
