import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from staging.common.services.connection import get_staging_db, get_production_db

class DynamicMerger:
    EXCLUDED_COLUMNS = {"id"}

    def __init__(self, limit: int, dry_run: bool = False):
        self.limit = limit
        self.dry_run = dry_run
        self.staging_db = get_staging_db()
        self.production_db = get_production_db()
        self.stats = {
            "companies": 0,
            "officers": 0,
            "financials": 0
        }

    def _get_table_columns(self, db, table_name: str) -> set:
        query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %(table_name)s
        """
        rows = db.execute(query, {'table_name': table_name}, fetch=True)
        return {row['column_name'] for row in rows}

    def _upsert_records(self, table_name: str, records: list, conflict_keys: list):
        if not records:
            return 0
        
        # Get production columns
        prod_cols = self._get_table_columns(self.production_db, table_name)
        if not prod_cols:
            print(f"Warning: Could not find columns for {table_name} in production.")
            return 0

        # Intersect with staging columns (keys from the first record)
        staging_cols = set(records[0].keys())
        shared_cols = list(
            prod_cols.intersection(staging_cols) - self.EXCLUDED_COLUMNS
        )
        
        if not shared_cols:
            print(f"Warning: No shared columns found for {table_name}.")
            return 0

        # Build query
        cols_str = ", ".join(shared_cols)
        vals_str = ", ".join([f"%({c})s" for c in shared_cols])
        
        update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in shared_cols if c not in conflict_keys])
        
        query = f"INSERT INTO {table_name} ({cols_str}) VALUES ({vals_str})"
        
        if update_set:
            conflict_str = ", ".join(conflict_keys)
            query += f" ON CONFLICT ({conflict_str}) DO UPDATE SET {update_set}"
        else:
            conflict_str = ", ".join(conflict_keys)
            query += f" ON CONFLICT ({conflict_str}) DO NOTHING"

        if self.dry_run:
            print(f"  [Dry Run] Would execute upsert for {len(records)} records into {table_name}")
            return len(records)

        inserted_or_updated = 0
        for record in records:
            # ensure we only pass data for the shared_cols
            data = {k: record[k] for k in shared_cols}
            try:
                self.production_db.execute(query, data, fetch=False)
                inserted_or_updated += 1
            except Exception as e:
                print(f"Error inserting record into {table_name}: {e}")

        return inserted_or_updated

    def run(self):
        print(f"Starting dynamic merge for up to {self.limit} companies...")
        
        # 1. Find companies that exist in staging_companies, staging_financials, and staging_officers
        target_query = """
            SELECT c.company_number
            FROM staging_companies c
            INNER JOIN staging_financials f ON c.company_number = f.company_number
            INNER JOIN staging_officers o ON c.company_number = o.company_number
            GROUP BY c.company_number
            LIMIT %(limit)s
        """
        target_rows = self.staging_db.execute(target_query, {'limit': self.limit}, fetch=True)
        if not target_rows:
            print("No companies found matching the criteria.")
            return
            
        company_numbers = [row['company_number'] for row in target_rows]
        print(f"Found {len(company_numbers)} companies to merge.")

        if not company_numbers:
            return

        # Prepare IN clause placeholders (e.g. %s, %s, %s)
        in_placeholders = tuple(company_numbers)
        format_strings = ','.join(['%s'] * len(company_numbers))

        # 2. Extract and merge companies
        print("Extracting and merging companies...")
        comp_query = f"SELECT * FROM staging_companies WHERE company_number IN ({format_strings})"
        companies = self.staging_db.execute(comp_query, in_placeholders, fetch=True)
        self.stats["companies"] = self._upsert_records("production_companies", companies, ["company_number"])
        print(f"  Merged {self.stats['companies']} companies.")

        # 3. Extract and merge officers
        print("Extracting and merging officers...")
        officle_query = f"SELECT * FROM staging_officers WHERE company_number IN ({format_strings})"
        officers = self.staging_db.execute(officle_query, in_placeholders, fetch=True)
        # Note: conflict targets based on typical schema
        self.stats["officers"] = self._upsert_records(
            "production_officers", 
            officers, 
            ["company_number", "officer_name", "appointed_on", "officer_role", "date_of_birth"]
        )
        print(f"  Merged {self.stats['officers']} officers.")

        # 4. Extract and merge financials
        print("Extracting and merging financials...")
        fin_query = f"SELECT * FROM staging_financials WHERE company_number IN ({format_strings})"
        financials = self.staging_db.execute(fin_query, in_placeholders, fetch=True)
        # Note: conflict targets based on typical schema
        self.stats["financials"] = self._upsert_records(
            "production_financials", 
            financials, 
            ["company_number", "period_end"]
        )
        print(f"  Merged {self.stats['financials']} financials.")
        
        print("\nMerge complete. Summary:")
        print(f"  Companies: {self.stats['companies']}")
        print(f"  Officers: {self.stats['officers']}")
        print(f"  Financials: {self.stats['financials']}")

def main():
    parser = argparse.ArgumentParser(description="Dynamic Merge Script for migrating N companies with officers and financials from staging to production.")
    parser.add_argument("--limit", type=int, required=True, help="Number of companies to dynamically extract and merge.")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without executing production inserts.")
    
    args = parser.parse_args()
    
    merger = DynamicMerger(limit=args.limit, dry_run=args.dry_run)
    merger.run()

if __name__ == "__main__":
    main()
