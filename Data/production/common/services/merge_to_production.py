"""
Merge validated data from staging to production database.
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from staging.common.services.connection import get_staging_db, get_production_db
from .validators import DataValidator, DataTransformer


class ProductionMerger:
    """Manage merging data from staging to production."""

    def __init__(self, batch_id: str, dry_run: bool = False):
        """
        Initialize merger.

        Args:
            batch_id: Batch ID to merge from staging
            dry_run: If True, only report what would be done
        """
        self.batch_id = batch_id
        self.dry_run = dry_run
        self.staging_db = get_staging_db()
        self.production_db = get_production_db()

        self.stats = {
            "companies_merged": 0,
            "companies_updated": 0,
            "officers_merged": 0,
            "financials_merged": 0,
            "contacts_merged": 0,
        }

    def merge_batch(self) -> dict:
        """
        Merge a validated batch from staging to production.

        Returns:
            Dictionary with merge statistics
        """
        print(f"[merge] {'DRY RUN: ' if self.dry_run else ''}Merging batch: {self.batch_id}")

        # Step 1: Validate data quality
        print("\n[1/5] Validating data quality...")
        validator = DataValidator(self.batch_id)
        validation_results = validator.validate_batch()

        if validation_results["quality_score"] < 0.7:
            print(
                f"  ✗ Quality score too low: {validation_results['quality_score']:.2%}"
            )
            print("  ! Fix validation issues before merging")
            for issue in validation_results["issues"]:
                if issue["severity"] == "error":
                    print(f"    - {issue['message']}")
            return {"status": "failed", "reason": "quality_score_too_low"}

        print(f"  ✓ Quality score: {validation_results['quality_score']:.2%}")

        # Step 2: Merge companies
        print("\n[2/5] Merging companies...")
        self._merge_companies()

        # Step 3: Merge officers
        print("\n[3/5] Merging officers...")
        self._merge_officers()

        # Step 4: Merge financials (if any)
        print("\n[4/5] Merging financials...")
        self._merge_financials()

        # Step 5: Log the merge
        print("\n[5/5] Logging merge...")
        if not self.dry_run:
            self._log_merge()

        # Print summary
        print(f"\n[merge] {'DRY RUN ' if self.dry_run else ''}Summary:")
        print(f"  Companies merged: {self.stats['companies_merged']}")
        print(f"  Companies updated: {self.stats['companies_updated']}")
        print(f"  Officers merged: {self.stats['officers_merged']}")
        print(f"  Financials merged: {self.stats['financials_merged']}")

        return {"status": "success", "stats": self.stats}

    def _merge_companies(self) -> None:
        """Merge companies from staging to production."""
        # Get companies from staging
        query = """
            SELECT *
            FROM staging_companies
            WHERE batch_id = %(batch_id)s
            AND needs_review = false
            AND company_number IS NOT NULL
            AND company_name IS NOT NULL
        """

        companies = self.staging_db.execute(query, {"batch_id": self.batch_id}, fetch=True)
        print(f"  Found {len(companies)} companies to merge")

        if self.dry_run:
            self.stats["companies_merged"] = len(companies)
            return

        for company in companies:
            # Calculate quality score
            quality_score = DataTransformer.calculate_company_quality_score(company)

            # Prepare data
            primary_sic = DataTransformer.extract_primary_sic(company["sic_codes"])

            # Upsert into production (insert or update)
            upsert_query = """
                INSERT INTO production_companies (
                    company_number,
                    company_name,
                    company_type,
                    company_status,
                    locality,
                    postal_code,
                    address_line_1,
                    address_line_2,
                    region,
                    country,
                    sic_codes,
                    primary_sic_code,
                    incorporation_date,
                    accounts_last_made_up_date,
                    accounts_ref_date,
                    accounts_next_due_date,
                    account_category,
                    returns_next_due_date,
                    returns_last_made_up_date,
                    num_mort_charges,
                    num_mort_outstanding,
                    num_mort_part_satisfied,
                    previous_names,
                    conf_stm_next_due_date,
                    conf_stm_last_made_up_date,
                    raw_data,
                    data_quality_score,
                    source_batch_id,
                    first_seen,
                    last_updated
                ) VALUES (
                    %(company_number)s,
                    %(company_name)s,
                    %(company_type)s,
                    %(company_status)s,
                    %(locality)s,
                    %(postal_code)s,
                    %(address_line_1)s,
                    %(address_line_2)s,
                    %(region)s,
                    %(country)s,
                    %(sic_codes)s,
                    %(primary_sic_code)s,
                    %(incorporation_date)s,
                    %(accounts_last_made_up_date)s,
                    %(accounts_ref_date)s,
                    %(accounts_next_due_date)s,
                    %(account_category)s,
                    %(returns_next_due_date)s,
                    %(returns_last_made_up_date)s,
                    %(num_mort_charges)s,
                    %(num_mort_outstanding)s,
                    %(num_mort_part_satisfied)s,
                    %(previous_names)s,
                    %(conf_stm_next_due_date)s,
                    %(conf_stm_last_made_up_date)s,
                    %(raw_data)s,
                    %(quality_score)s,
                    %(batch_id)s,
                    NOW(),
                    NOW()
                )
                ON CONFLICT (company_number) DO UPDATE SET
                    company_name = EXCLUDED.company_name,
                    company_type = EXCLUDED.company_type,
                    company_status = EXCLUDED.company_status,
                    locality = EXCLUDED.locality,
                    postal_code = EXCLUDED.postal_code,
                    region = EXCLUDED.region,
                    country = EXCLUDED.country,
                    sic_codes = EXCLUDED.sic_codes,
                    primary_sic_code = EXCLUDED.primary_sic_code,
                    incorporation_date = EXCLUDED.incorporation_date,
                    accounts_last_made_up_date = EXCLUDED.accounts_last_made_up_date,
                    accounts_ref_date = EXCLUDED.accounts_ref_date,
                    accounts_next_due_date = EXCLUDED.accounts_next_due_date,
                    account_category = EXCLUDED.account_category,
                    returns_next_due_date = EXCLUDED.returns_next_due_date,
                    returns_last_made_up_date = EXCLUDED.returns_last_made_up_date,
                    num_mort_charges = EXCLUDED.num_mort_charges,
                    num_mort_outstanding = EXCLUDED.num_mort_outstanding,
                    num_mort_part_satisfied = EXCLUDED.num_mort_part_satisfied,
                    previous_names = EXCLUDED.previous_names,
                    conf_stm_next_due_date = EXCLUDED.conf_stm_next_due_date,
                    conf_stm_last_made_up_date = EXCLUDED.conf_stm_last_made_up_date,
                    raw_data = EXCLUDED.raw_data,
                    data_quality_score = EXCLUDED.data_quality_score,
                    source_batch_id = EXCLUDED.source_batch_id,
                    last_updated = NOW()
                RETURNING (xmax = 0) AS inserted
            """

            result = self.production_db.execute(
                upsert_query,
                {
                    "company_number": company["company_number"],
                    "company_name": company["company_name"],
                    "company_type": company["company_type"],
                    "company_status": company["company_status"],
                    "locality": company["locality"],
                    "postal_code": company["postal_code"],
                    "address_line_1": company["address_line_1"],
                    "address_line_2": company["address_line_2"],
                    "region": company["region"],
                    "country": company["country"],
                    "sic_codes": company["sic_codes"],
                    "primary_sic_code": primary_sic,
                    "incorporation_date": company["incorporation_date"],
                    "accounts_last_made_up_date": company["accounts_last_made_up_date"],
                    "accounts_ref_date": company["accounts_ref_date"],
                    "accounts_next_due_date": company["accounts_next_due_date"],
                    "account_category": company["account_category"],
                    "returns_next_due_date": company["returns_next_due_date"],
                    "returns_last_made_up_date": company["returns_last_made_up_date"],
                    "num_mort_charges": company["num_mort_charges"],
                    "num_mort_outstanding": company["num_mort_outstanding"],
                    "num_mort_part_satisfied": company["num_mort_part_satisfied"],
                    "previous_names": company["previous_names"],
                    "conf_stm_next_due_date": company["conf_stm_next_due_date"],
                    "conf_stm_last_made_up_date": company["conf_stm_last_made_up_date"],
                    "raw_data": company["raw_data"],
                    "quality_score": quality_score,
                    "batch_id": self.batch_id,
                },
                fetch=True,
            )

            if result and result[0]["inserted"]:
                self.stats["companies_merged"] += 1
            else:
                self.stats["companies_updated"] += 1

        print(f"  ✓ Merged {self.stats['companies_merged']} new companies")
        print(f"  ✓ Updated {self.stats['companies_updated']} existing companies")

    def _merge_officers(self) -> None:
        """Merge officers from staging to production."""
        # Get officers for companies in this batch
        query = """
            SELECT
                so.*
            FROM staging_officers so
            JOIN staging_companies sc ON so.company_number = sc.company_number
            WHERE sc.batch_id = %(batch_id)s
            AND sc.needs_review = false
            AND so.officer_name IS NOT NULL
        """

        officers = self.staging_db.execute(query, {"batch_id": self.batch_id}, fetch=True)
        print(f"  Found {len(officers)} officers to merge")

        if self.dry_run:
            self.stats["officers_merged"] = len(officers)
            return

        for officer in officers:

            # Upsert officer
            upsert_query = """
                INSERT INTO production_officers (
                    company_number,
                    officer_name,
                    officer_role,
                    appointed_on,
                    resigned_on,
                    nationality,
                    nature_of_control,
                    date_of_birth,
                    address_line_1,
                    address_line_2,
                    locality,
                    postal_code,
                    country,
                    raw_data,
                    source_batch_id,
                    first_seen,
                    last_updated
                ) VALUES (
                    %(company_number)s,
                    %(officer_name)s,
                    %(officer_role)s,
                    %(appointed_on)s,
                    %(resigned_on)s,
                    %(nationality)s,
                    %(nature_of_control)s,
                    %(date_of_birth)s,
                    %(address_line_1)s,
                    %(address_line_2)s,
                    %(locality)s,
                    %(postal_code)s,
                    %(country)s,
                    %(raw_data)s,
                    %(batch_id)s,
                    NOW(),
                    NOW()
                )
                ON CONFLICT (company_number, officer_name, appointed_on, officer_role, date_of_birth) DO UPDATE SET
                    resigned_on = EXCLUDED.resigned_on,
                    nationality = EXCLUDED.nationality,
                    nature_of_control = EXCLUDED.nature_of_control,
                    date_of_birth = EXCLUDED.date_of_birth,
                    raw_data = EXCLUDED.raw_data,
                    source_batch_id = EXCLUDED.source_batch_id,
                    last_updated = NOW()
            """

            self.production_db.execute(
                upsert_query,
                {
                    "company_number": officer["company_number"],
                    "officer_name": officer["officer_name"],
                    "officer_role": officer["officer_role"] or "unknown",
                    "appointed_on": officer["appointed_on"],
                    "resigned_on": officer["resigned_on"],
                    "nationality": officer["nationality"],
                    "nature_of_control": officer.get("nature_of_control"),
                    "date_of_birth": officer.get("date_of_birth"),
                    "address_line_1": officer["address_line_1"],
                    "address_line_2": officer["address_line_2"],
                    "locality": officer["locality"],
                    "postal_code": officer["postal_code"],
                    "country": officer["country"],
                    "raw_data": officer["raw_data"],
                    "batch_id": self.batch_id,
                },
            )

            self.stats["officers_merged"] += 1

        print(f"  ✓ Merged {self.stats['officers_merged']} officers")

    def _merge_financials(self) -> None:
        """Merge financials from staging to production (if any exist)."""
        # Get financials for companies in this batch
        query = """
            SELECT
                sf.*
            FROM staging_financials sf
            JOIN staging_companies sc ON sf.company_number = sc.company_number
            WHERE sc.batch_id = %(batch_id)s
            AND sc.needs_review = false
        """

        financials = self.staging_db.execute(query, {"batch_id": self.batch_id}, fetch=True)
        print(f"  Found {len(financials)} financial records to merge")

        if len(financials) == 0:
            return

        if self.dry_run:
            self.stats["financials_merged"] = len(financials)
            return

        for financial in financials:
            # Upsert financial record
            upsert_query = """
                INSERT INTO production_financials (
                    company_number,
                    period_start,
                    period_end,
                    turnover,
                    profit_after_tax,
                    total_assets,
                    total_liabilities,
                    net_worth,
                    source,
                    raw_data,
                    source_batch_id
                ) VALUES (
                    %(company_number)s,
                    %(period_start)s,
                    %(period_end)s,
                    %(turnover)s,
                    %(profit_after_tax)s,
                    %(total_assets)s,
                    %(total_liabilities)s,
                    %(net_worth)s,
                    %(source)s,
                    %(raw_data)s,
                    %(batch_id)s
                )
                ON CONFLICT (company_number, period_end) DO UPDATE SET
                    turnover = EXCLUDED.turnover,
                    profit_after_tax = EXCLUDED.profit_after_tax,
                    total_assets = EXCLUDED.total_assets,
                    total_liabilities = EXCLUDED.total_liabilities,
                    net_worth = EXCLUDED.net_worth,
                    source = EXCLUDED.source,
                    raw_data = EXCLUDED.raw_data,
                    source_batch_id = EXCLUDED.source_batch_id,
                    last_updated = NOW()
            """

            self.production_db.execute(
                upsert_query,
                {
                    "company_number": financial["company_number"],
                    "period_start": financial["period_start"],
                    "period_end": financial["period_end"],
                    "turnover": financial["turnover"],
                    "profit_after_tax": financial["profit_loss"],
                    "total_assets": financial["total_assets"],
                    "total_liabilities": financial["total_liabilities"],
                    "net_worth": financial["net_worth"],
                    "source": financial["source"] or "api",
                    "raw_data": financial["raw_data"],
                    "batch_id": self.batch_id,
                },
            )

            self.stats["financials_merged"] += 1

        print(f"  ✓ Merged {self.stats['financials_merged']} financial records")

    def _log_merge(self) -> None:
        """Log merge operation in production database."""
        query = """
            INSERT INTO merge_log (
                batch_id,
                companies_merged,
                officers_merged,
                financials_merged,
                contacts_merged,
                merged_by,
                notes
            ) VALUES (
                %(batch_id)s,
                %(companies_merged)s,
                %(officers_merged)s,
                %(financials_merged)s,
                %(contacts_merged)s,
                %(merged_by)s,
                %(notes)s
            )
        """

        self.production_db.execute(
            query,
            {
                "batch_id": self.batch_id,
                "companies_merged": self.stats["companies_merged"],
                "officers_merged": self.stats["officers_merged"],
                "financials_merged": self.stats["financials_merged"],
                "contacts_merged": self.stats["contacts_merged"],
                "merged_by": "merge_to_production.py",
                "notes": f"Merged {self.stats['companies_merged']} companies, {self.stats['officers_merged']} officers",
            },
        )

        print(f"  ✓ Logged merge to production")


def list_batches() -> None:
    """List all batches available for merging."""
    staging_db = get_staging_db()
    
    # Need to check production for merge status
    merged_batches = set()
    try:
        production_db = get_production_db()
        rows = production_db.execute("SELECT batch_id FROM merge_log", fetch=True)
        merged_batches = {row["batch_id"] for row in rows}
    except Exception:
        # Fails if tables don't exist yet etc.
        pass

    query = """
        SELECT
            il.batch_id,
            il.search_name,
            il.started_at,
            il.completed_at,
            il.companies_count,
            il.status
        FROM staging_ingestion_log il
        WHERE il.status = 'completed'
        ORDER BY il.completed_at DESC
        LIMIT 20
    """

    batches = staging_db.execute(query, fetch=True)

    print("\nAvailable batches for merging:")
    print("-" * 120)
    print(
        f"{'Batch ID':<50} {'Search Name':<20} {'Companies':<12} {'Status'}"
    )
    print("-" * 120)

    for batch in batches:
        is_merged = batch["batch_id"] in merged_batches
        status = "merged" if is_merged else "not_merged"
        print(
            f"{batch['batch_id']:<50} {batch['search_name']:<20} "
            f"{batch['companies_count']:<12} "
            f"{status}"
        )


def main() -> None:
    """Main entry point for merge script."""
    import argparse

    parser = argparse.ArgumentParser(description="Merge staging data to production")
    parser.add_argument("--batch-id", help="Batch ID to merge")
    parser.add_argument("--list", action="store_true", help="List available batches")
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be done without doing it"
    )

    args = parser.parse_args()

    if args.list:
        list_batches()
        return

    if not args.batch_id:
        print("Error: --batch-id required (or use --list to see available batches)")
        list_batches()
        return

    # Run the merge
    merger = ProductionMerger(args.batch_id, dry_run=args.dry_run)
    result = merger.merge_batch()

    if result["status"] == "success":
        print(f"\n{'DRY RUN: ' if args.dry_run else ''}✓ Merge completed successfully")
    else:
        print(f"\n✗ Merge failed: {result.get('reason', 'unknown')}")
        sys.exit(1)


if __name__ == "__main__":
    main()
