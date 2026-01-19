import sys
import logging
from pathlib import Path
import psycopg2

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parents[3]))
sys.path.insert(0, str(Path(r"q:\development\Data-co")))
# Add Data/database in case of implicit imports
sys.path.insert(0, str(Path(r"q:\development\Data-co\Data\database")))

try:
    from Data.database.connection import get_staging_db
except ImportError:
    # Fallback or try direct import if path setup is weird
    try:
        from connection import get_staging_db
    except ImportError as e:
        print(f"Import failed: {e}")
        sys.exit(1)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def apply_schema():
    schema_path = Path(r"q:\development\Data-co\Data\database\schema_staging.sql")
    if not schema_path.exists():
        logger.error(f"Schema file not found: {schema_path}")
        return

    try:
        with open(schema_path, 'r') as f:
            sql = f.read()
            
        db_mgr = get_staging_db()
        with db_mgr.get_connection() as conn:
            with conn.cursor() as cur:
                logger.info("Applying schema...")
                cur.execute(sql)
                conn.commit()
                logger.info("Schema applied successfully.")
        # db_mgr.close() # Don't close global pool if singleton usage, but explicit close ok here.
        # But get_staging_db returns global singleton. Closing it might affect others if parallel.
        # Script ends anyway.
        
    except Exception as e:
        logger.error(f"Failed to apply schema: {e}")

if __name__ == "__main__":
    apply_schema()
