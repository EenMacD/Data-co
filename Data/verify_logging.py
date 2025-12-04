
import sys
import os
from pathlib import Path
from unittest.mock import MagicMock

# Setup path to mimic api-main-db.py
ROOT = Path("/Users/iainmcdulling/Acacia/data-co/Data")
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# Add path to api-main-db.py
SCRIPT_DIR = ROOT / "Data-injestion-workflows/Api-request-workflow"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.append(str(SCRIPT_DIR))

# Mock database modules
sys.modules["database"] = MagicMock()
sys.modules["database.inserters"] = MagicMock()
sys.modules["database.connection"] = MagicMock()
sys.modules["psycopg2"] = MagicMock()

from ingestion_config.loader import load_config

# Import the function from api-main-db.py
import importlib.util
spec = importlib.util.spec_from_file_location("api_main_db", SCRIPT_DIR / "api-main-db.py")
api_main_db = importlib.util.module_from_spec(spec)
sys.modules["api_main_db"] = api_main_db
spec.loader.exec_module(api_main_db)

def check_logging():
    try:
        config = load_config()
        criteria = config.search_criteria
        tech_config = config.technical_config
        
        print("Checking logging...")
        # Ensure we can find the data
        api_main_db.ROOT = ROOT
        
        # This should print "No limit configured"
        df = api_main_db._find_and_filter_companies(criteria, tech_config)
        
    except Exception as e:
        print(f"Error checking logging: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_logging()
