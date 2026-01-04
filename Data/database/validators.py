"""
Data validation and quality checks for staging data before merging to production.
"""
from __future__ import annotations

import re
from typing import Any

from database.connection import get_staging_db


class DataValidator:
    """Validate and score data quality in staging database."""

    def __init__(self, batch_id: str | None = None):
        """
        Initialize validator.

        Args:
            batch_id: If provided, only validate this batch
        """
        self.db = get_staging_db()
        self.batch_id = batch_id
        self.issues = []

    def validate_batch(self) -> dict:
        """
        Run all validation checks on a batch.

        Returns:
            Dict with validation results and quality score
        """
        print(f"[validator] Validating batch: {self.batch_id or 'all batches'}")

        results = {
            "batch_id": self.batch_id,
            "companies_checked": 0,
            "officers_checked": 0,
            "issues": [],
            "quality_score": 0.0,
        }

        # Check company data quality
        company_issues = self._validate_companies()
        results["companies_checked"] = company_issues["total"]
        results["issues"].extend(company_issues["issues"])

        # Check officer data quality
        officer_issues = self._validate_officers()
        results["officers_checked"] = officer_issues["total"]
        results["issues"].extend(officer_issues["issues"])

        # Calculate overall quality score
        total_checks = len(results["issues"])
        failed_checks = sum(1 for issue in results["issues"] if issue["severity"] == "error")
        results["quality_score"] = (
            1.0 - (failed_checks / total_checks) if total_checks > 0 else 1.0
        )

        print(f"  - Quality score: {results['quality_score']:.2%}")
        print(f"  - Issues found: {len(results['issues'])}")

        return results

    def _validate_companies(self) -> dict:
        """Validate company records."""
        where_clause = f"WHERE batch_id = '{self.batch_id}'" if self.batch_id else ""

        # Check for missing required fields
        query = f"""
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE company_number IS NULL OR company_number = '') as missing_number,
                COUNT(*) FILTER (WHERE company_name IS NULL OR company_name = '') as missing_name,
                COUNT(*) FILTER (WHERE company_status IS NULL OR company_status = '') as missing_status,
                COUNT(*) FILTER (WHERE locality IS NULL OR locality = '') as missing_locality,
                COUNT(*) FILTER (WHERE sic_codes IS NULL OR array_length(sic_codes, 1) = 0) as missing_sic,
                COUNT(*) FILTER (WHERE LENGTH(company_number) != 8) as invalid_number_format
            FROM staging_companies
            {where_clause}
        """

        result = self.db.execute(query, fetch=True)[0]

        issues = []
        total = result["total"]

        if result["missing_number"] > 0:
            issues.append({
                "severity": "error",
                "type": "missing_company_number",
                "count": result["missing_number"],
                "message": f"{result['missing_number']} companies missing company number",
            })

        if result["invalid_number_format"] > 0:
            issues.append({
                "severity": "error",
                "type": "invalid_company_number",
                "count": result["invalid_number_format"],
                "message": f"{result['invalid_number_format']} companies have invalid number format (not 8 chars)",
            })

        if result["missing_name"] > 0:
            issues.append({
                "severity": "error",
                "type": "missing_company_name",
                "count": result["missing_name"],
                "message": f"{result['missing_name']} companies missing name",
            })

        if result["missing_status"] > 0:
            issues.append({
                "severity": "warning",
                "type": "missing_status",
                "count": result["missing_status"],
                "message": f"{result['missing_status']} companies missing status",
            })

        if result["missing_locality"] > 0:
            issues.append({
                "severity": "warning",
                "type": "missing_locality",
                "count": result["missing_locality"],
                "message": f"{result['missing_locality']} companies missing locality",
            })

        if result["missing_sic"] > 0:
            issues.append({
                "severity": "info",
                "type": "missing_sic",
                "count": result["missing_sic"],
                "message": f"{result['missing_sic']} companies missing SIC codes",
            })

        return {"total": total, "issues": issues}

    def _validate_officers(self) -> dict:
        """Validate officer records."""
        join_clause = ""
        if self.batch_id:
            # JOIN using company_number (FK) instead of id
            join_clause = f"JOIN staging_companies sc ON so.company_number = sc.company_number WHERE sc.batch_id = '{self.batch_id}'"

        query = f"""
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE so.officer_name IS NULL OR so.officer_name = '') as missing_name,
                COUNT(*) FILTER (WHERE so.officer_role IS NULL OR so.officer_role = '') as missing_role,
                COUNT(*) FILTER (WHERE so.appointed_on IS NULL) as missing_appointment
            FROM staging_officers so
            {join_clause}
        """

        result = self.db.execute(query, fetch=True)[0]

        issues = []
        total = result["total"]

        if result["missing_name"] > 0:
            issues.append({
                "severity": "warning",
                "type": "missing_officer_name",
                "count": result["missing_name"],
                "message": f"{result['missing_name']} officers missing name",
            })

        if result["missing_role"] > 0:
            issues.append({
                "severity": "warning",
                "type": "missing_officer_role",
                "count": result["missing_role"],
                "message": f"{result['missing_role']} officers missing role",
            })

        if result["missing_appointment"] > 0:
            issues.append({
                "severity": "info",
                "type": "missing_appointment_date",
                "count": result["missing_appointment"],
                "message": f"{result['missing_appointment']} officers missing appointment date",
            })

        return {"total": total, "issues": issues}

    def mark_for_review(self, review_criteria: dict) -> int:
        """
        Mark records for manual review based on criteria.

        Args:
            review_criteria: Dict with validation rules

        Returns:
            Number of records marked for review
        """
        where_clause = f"WHERE batch_id = '{self.batch_id}'" if self.batch_id else ""

        # Mark companies with missing critical data
        query = f"""
            UPDATE staging_companies
            SET needs_review = true,
                review_notes = 'Missing critical data: ' ||
                    CASE
                        WHEN company_number IS NULL THEN 'company_number '
                        ELSE ''
                    END ||
                    CASE
                        WHEN company_name IS NULL THEN 'company_name '
                        ELSE ''
                    END
            {where_clause}
            AND (company_number IS NULL OR company_name IS NULL)
        """

        result = self.db.execute(query, fetch=False)
        print(f"[validator] Marked records for review")
        return 0

    def get_review_queue(self) -> list[dict]:
        """Get all records marked for review."""
        where_clause = f"WHERE batch_id = '{self.batch_id}'" if self.batch_id else ""

        query = f"""
            SELECT * FROM staging_review_queue
            {where_clause}
            ORDER BY ingested_at DESC
            LIMIT 100
        """

        return self.db.execute(query, fetch=True)


class DataTransformer:
    """Transform and normalize data before merging to production."""

    @staticmethod
    def normalize_officer_name(name: str | None) -> str | None:
        """
        Normalize officer name for matching.

        Args:
            name: Raw officer name

        Returns:
            Normalized name (lowercase, no punctuation)
        """
        if not name:
            return None

        # Convert to lowercase
        normalized = name.lower()

        # Remove punctuation
        normalized = re.sub(r"[^\w\s]", "", normalized)

        # Remove extra whitespace
        normalized = " ".join(normalized.split())

        return normalized

    @staticmethod
    def extract_primary_sic(sic_codes: list[str] | None) -> str | None:
        """Extract primary (first) SIC code."""
        if not sic_codes or len(sic_codes) == 0:
            return None
        return sic_codes[0]

    @staticmethod
    def calculate_company_quality_score(company_data: dict) -> float:
        """
        Calculate data quality score for a company (0.0 to 1.0).

        Args:
            company_data: Company record from staging

        Returns:
            Quality score
        """
        score = 0.0
        max_score = 10.0

        # Required fields (3 points each)
        if company_data.get("company_number"):
            score += 3.0
        if company_data.get("company_name"):
            score += 3.0

        # Important fields (1 point each)
        if company_data.get("company_status"):
            score += 1.0
        if company_data.get("locality"):
            score += 1.0
        if company_data.get("sic_codes"):
            score += 1.0

        # Nice to have (0.5 points each)
        if company_data.get("postal_code"):
            score += 0.5
        if company_data.get("region"):
            score += 0.5

        return min(score / max_score, 1.0)


if __name__ == "__main__":
    # Test validator
    print("Testing data validator...")

    try:
        # Get latest batch
        db = get_staging_db()
        latest_batch = db.execute(
            "SELECT batch_id FROM staging_ingestion_log ORDER BY started_at DESC LIMIT 1",
            fetch=True,
        )

        if latest_batch:
            batch_id = latest_batch[0]["batch_id"]
            print(f"Testing on batch: {batch_id}")

            validator = DataValidator(batch_id)
            results = validator.validate_batch()

            print(f"\nValidation results:")
            print(f"  Companies checked: {results['companies_checked']}")
            print(f"  Officers checked: {results['officers_checked']}")
            print(f"  Quality score: {results['quality_score']:.2%}")
            print(f"\nIssues:")
            for issue in results["issues"]:
                print(f"  [{issue['severity']}] {issue['message']}")

            # Test name normalization
            test_name = "O'Brien, John-Paul (Dr.)"
            normalized = DataTransformer.normalize_officer_name(test_name)
            print(f"\nName normalization test:")
            print(f"  Original: {test_name}")
            print(f"  Normalized: {normalized}")

        else:
            print("No batches found in staging database")

    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
