"""
Apply database migrations to staging database.

Usage:
    python database/apply_migrations.py
"""
from pathlib import Path
import sys

# Add parent path for imports
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from database.connection import get_staging_db


def apply_migration(db, migration_file: Path):
    """Apply a single migration file."""
    print(f"\n[migration] Applying {migration_file.name}...")

    with open(migration_file, 'r') as f:
        sql = f.read()

    # Execute entire file as one transaction
    # This properly handles DO $$ blocks and other complex SQL
    with db.get_cursor(dict_cursor=False) as cur:
        print(f"  Executing migration...")
        cur.execute(sql)

    print(f"[migration] ✓ {migration_file.name} applied successfully")


def main():
    """Apply all migrations in order."""
    migrations_dir = Path(__file__).parent / 'migrations'

    if not migrations_dir.exists():
        print(f"[error] Migrations directory not found: {migrations_dir}")
        sys.exit(1)

    # Get all .sql files sorted by name
    migration_files = sorted(migrations_dir.glob('*.sql'))

    if not migration_files:
        print("[info] No migration files found")
        return

    print(f"[info] Found {len(migration_files)} migration(s)")

    # Connect to database
    db = get_staging_db()
    print(f"[info] Connected to staging database")

    # Apply migrations
    for migration_file in migration_files:
        try:
            apply_migration(db, migration_file)
        except Exception as e:
            print(f"[error] Failed to apply {migration_file.name}: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

    print(f"\n[success] All {len(migration_files)} migration(s) applied successfully!")

    # Verify schema
    print("\n[info] Verifying schema...")
    with db.get_cursor() as cur:
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        tables = cur.fetchall()
        print(f"[info] Tables in database: {', '.join(t['table_name'] for t in tables)}")

        # Check staging_companies columns
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'staging_companies'
            ORDER BY ordinal_position
        """)
        columns = cur.fetchall()
        print(f"\n[info] staging_companies columns:")
        for col in columns:
            print(f"  - {col['column_name']} ({col['data_type']})")


if __name__ == "__main__":
    main()
