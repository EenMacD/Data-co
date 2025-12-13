
import sys
from pathlib import Path

# Setup path to mimic api-main-db.py
ROOT = Path("/Users/iainmcdulling/Acacia/data-co/Data")
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from ingestion_config.loader import load_config

def check_config():
    try:
        config = load_config()
        criteria = config.search_criteria
        limit = criteria.get("selection", {}).get("limit")
        print(f"Loaded limit: {limit}")
        
        tech_config = config.technical_config
        rate_limit = tech_config.get("api_enrichment", {}).get("request_rate_per_minute")
        print(f"Loaded rate_limit: {rate_limit}")
        
    except Exception as e:
        print(f"Error loading config: {e}")

if __name__ == "__main__":
    check_config()
