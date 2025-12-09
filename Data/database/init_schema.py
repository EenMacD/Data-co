"""
Initialize staging database schema.

This should be run BEFORE apply_migrations.py
It creates the base tables that migrations will modify.

Usage:
    python database/init_schema.py
"""
from pathlib import Path
import sys

# Add parent path for imports
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from database.connection import get_staging_db


def init_schema():
    """Initialize staging database schema."""
    schema_file = Path(__file__).parent / 'schema_staging.sql'

    if not schema_file.exists():
        print(f"[error] Schema file not found: {schema_file}")
        sys.exit(1)

    print(f"[info] Reading schema from {schema_file.name}")

    with open(schema_file, 'r') as f:
        sql = f.read()

    # Connect to database
    db = get_staging_db()
    print(f"[info] Connected to staging database")

    # Execute schema
    print(f"[info] Creating tables...")
    with db.get_cursor(dict_cursor=False) as cur:
        cur.execute(sql)

    print(f"[success] ✓ Schema initialized successfully!")

    # Verify tables were created
    print("\n[info] Verifying schema...")
    with db.get_cursor() as cur:
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        tables = cur.fetchall()
        print(f"[info] Tables created: {', '.join(t['table_name'] for t in tables)}")


if __name__ == "__main__":
    try:
        init_schema()
    except Exception as e:
        print(f"[error] Failed to initialize schema: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
