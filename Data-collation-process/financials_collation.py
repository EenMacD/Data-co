from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zipfile import ZipFile
from urllib.parse import urljoin

import pandas as pd
import requests
from dotenv import load_dotenv
from xml.etree import ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from ingestion_config.loader import load_config  # noqa: E402

DOCUMENT_API_BASE = "https://document-api.company-information.service.gov.uk"


# -----------------
# Helpers
# -----------------

def _resolve_path(path_value) -> Path:
    path = Path(path_value)
    return path if path.is_absolute() else ROOT / path


def _dt_utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_date_to_dt(value: str) -> Optional[datetime]:
    try:
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _safe_filename(name: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_", ".") else "_" for c in name).strip("._")


# -----------------
# Company selection (reuse bulk filter logic)
# -----------------

def _find_and_filter_companies(criteria: dict, tech_config: dict) -> pd.DataFrame:
    search_name = criteria.get("name", "default_search")
    base_path = _resolve_path(tech_config.get("storage", {}).get("base_path", "project_data/default"))
    raw_root = base_path.with_name(base_path.name.format(name=search_name)) / "raw"
    company_profile_dir = raw_root / "company_profile"

    if not company_profile_dir.is_dir():
        print(f"  ! Bulk company data not found at {company_profile_dir}")
        print("  ! Please run the bulk download workflow first.")
        return pd.DataFrame()

    try:
        latest_zip = max(company_profile_dir.glob("*.zip"), key=os.path.getmtime)
        print(f"  - Reading bulk data from: {latest_zip.name}")
    except ValueError:
        print(f"  ! No bulk ZIP file found in {company_profile_dir}")
        return pd.DataFrame()

    selection = criteria.get("selection", {})
    locality = selection.get("locality")
    status = selection.get("company_status")
    sic_include = selection.get("industry_codes", {}).get("include", [])
    limit = selection.get("limit")

    print(
        f"  - Applying filters: locality='{locality or 'any'}', "
        f"status='{status or 'any'}', sic_codes='{sic_include or 'any'}'"
    )

    all_chunks: List[pd.DataFrame] = []
    with ZipFile(latest_zip, "r") as archive:
        csv_filename = next((n for n in archive.namelist() if n.lower().endswith(".csv")), None)
        if not csv_filename:
            print(f"  ! No CSV file found in {latest_zip.name}")
            return pd.DataFrame()
        with archive.open(csv_filename) as csv_file:
            chunk_iter = pd.read_csv(csv_file, chunksize=100_000, dtype=str, low_memory=False)
            for chunk in chunk_iter:
                chunk.columns = chunk.columns.str.strip()
                filtered = chunk.copy()
                if locality and "RegAddress.PostTown" in filtered.columns:
                    filtered = filtered[
                        filtered["RegAddress.PostTown"].str.contains(locality, case=False, na=False)
                    ]
                if status and "CompanyStatus" in filtered.columns:
                    filtered = filtered[filtered["CompanyStatus"].str.lower() == status.lower()]
                if sic_include:
                    sic_cols = [
                        f"SICCode.SicText_{i}" for i in range(1, 5) if f"SICCode.SicText_{i}" in filtered.columns
                    ]
                    if sic_cols:
                        mask = filtered[sic_cols].apply(
                            lambda row: any(str(cell).startswith(tuple(sic_include)) for cell in row), axis=1
                        )
                        filtered = filtered[mask]
                if not filtered.empty:
                    all_chunks.append(filtered)

    if not all_chunks:
        return pd.DataFrame()

    final_df = pd.concat(all_chunks)
    final_df.columns = final_df.columns.str.strip()
    print(f"  - Found {len(final_df)} companies matching criteria.")
    if limit:
        print(f"  - Applying limit of {limit} companies.")
        final_df = final_df.head(limit)
    return final_df[["CompanyNumber", "CompanyName"]]


# -----------------
# CH API helpers (optional download)
# -----------------

def _fetch_filing_history(session: requests.Session, company_number: str) -> Dict[str, Any]:
    url = f"https://api.companieshouse.gov.uk/company/{company_number}/filing-history"
    params = {"category": "accounts", "items_per_page": 100}
    resp = session.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _filter_since(items: List[Dict[str, Any]], since: datetime) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for it in items:
        dt = _iso_date_to_dt(it.get("date") or it.get("action_date") or "")
        if dt and dt >= since:
            out.append(it)
    return out


def _download_document(session: requests.Session, item: Dict[str, Any], out_dir: Path) -> Optional[Path]:
    links = item.get("links") or {}
    meta_url = links.get("document_metadata")
    if not meta_url:
        return None
    try:
        meta = session.get(urljoin(DOCUMENT_API_BASE, meta_url), timeout=30)
        meta.raise_for_status()
        d = meta.json()
        content_link = (d.get("links") or {}).get("document")
        if not content_link:
            return None
        # prefer PDF; fall back to default
        resp = session.get(
            urljoin(DOCUMENT_API_BASE, content_link),
            headers={"Accept": "application/pdf"},
            timeout=60,
        )
        if resp.status_code == 406:
            resp = session.get(
                urljoin(DOCUMENT_API_BASE, content_link), timeout=60
            )
        resp.raise_for_status()
        ctype = resp.headers.get("Content-Type", "").lower()
        # estimate extension
        if "xml" in ctype:
            ext = ".xml"
        elif "html" in ctype:
            ext = ".html"
        elif "pdf" in ctype:
            ext = ".pdf"
        else:
            ext = ".bin"
        period_end = (item.get("description_values") or {}).get("made_up_date") or item.get("date") or ""
        fname = _safe_filename(f"{period_end}_{item.get('type','doc')}{ext}")
        out_path = out_dir / fname
        with out_path.open("wb") as fh:
            fh.write(resp.content)
        return out_path
    except requests.RequestException as exc:
        print(f"      ! Document download failed: {exc}")
        return None


# -----------------
# XBRL / iXBRL parsing
# -----------------

_TAG_SYNONYMS: Dict[str, Tuple[str, ...]] = {
    "turnover": (
        "Turnover",
        "Revenue",
        "RevenueFromContractsWithCustomersExcludingExciseDuties",
    ),
    "profit_loss": (
        "ProfitLoss",
        "ProfitLossAccount",
        "ProfitLossBeforeTax",
        "ProfitLossFromOperatingActivities",
        "NetIncomeLoss",
    ),
    "operating_profit": (
        "OperatingProfitLoss",
        "ProfitLossFromOperatingActivities",
    ),
    "cash": (
        "CashBankInHand",
        "CashAndCashEquivalents",
        "CashAndCashEquivalentsAtCarryingValue",
    ),
    "total_assets": (
        "TotalAssets",
        "Assets",
        "AssetsTotal",
    ),
    "net_assets": (
        "NetAssets",
        "NetAssetsLiabilities",
        "NetAssetsLiabilitiesIncludingNoncontrollingInterests",
    ),
    "total_liabilities": (
        "Liabilities",
        "LiabilitiesTotal",
    ),
}


def _localname(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    if ":" in tag:
        return tag.split(":", 1)[1]
    return tag


def _parse_xbrl_xml(path: Path) -> List[Tuple[str, Dict[str, float]]]:
    """
    Return list of (period_end, facts) dicts.
    """
    out: Dict[str, Dict[str, float]] = {}
    try:
        tree = ET.parse(str(path))
        root = tree.getroot()
    except Exception as exc:
        print(f"      ! XBRL parse failed for {path.name}: {exc}")
        return []

    # build context id -> period_end
    ctx_end: Dict[str, str] = {}
    for ctx in root.findall('.//*{*}context'):
        ctx_id = ctx.attrib.get('id')
        if not ctx_id:
            continue
        # prefer endDate; fall back to instant
        end_el = ctx.find('.//*{*}endDate')
        inst_el = ctx.find('.//*{*}instant')
        end_val = (end_el.text if end_el is not None else None) or (inst_el.text if inst_el is not None else None)
        if end_val:
            ctx_end[ctx_id] = end_val

    # iterate facts (numbers)
    for el in root.iter():
        tag = _localname(el.tag)
        if not tag or tag in {"context", "schemaRef", "unit", "entity", "identifier", "period", "startDate", "endDate", "instant"}:
            continue
        text = (el.text or "").strip()
        if not text:
            continue
        # numeric only for now
        try:
            val = float(text.replace(",", ""))
        except Exception:
            continue
        ctx_ref = el.attrib.get("contextRef")
        period_end = ctx_end.get(ctx_ref, "unknown")
        # map to canonical key if possible
        canonical: Optional[str] = None
        for key, aliases in _TAG_SYNONYMS.items():
            if tag in aliases:
                canonical = key
                break
        if not canonical:
            continue
        bucket = out.setdefault(period_end, {})
        if canonical not in bucket:
            bucket[canonical] = val

    return [(k, v) for k, v in out.items()]


_IX_NONFRACTION_RE = re.compile(
    r"<ix:nonFraction\s+[^>]*?name=\"(?P<name>[^\"]+)\"[^>]*?contextRef=\"(?P<context>[^\"]+)\"[^>]*?>(?P<value>.*?)</ix:nonFraction>",
    re.IGNORECASE | re.DOTALL,
)
_IX_CONTEXT_RE = re.compile(
    r"<xbrli:context\s+[^>]*?id=\"(?P<id>[^\"]+)\"[^>]*?>.*?(?:<xbrli:endDate>(?P<end>[^<]+)</xbrli:endDate>|<xbrli:instant>(?P<inst>[^<]+)</xbrli:instant>).*?</xbrli:context>",
    re.IGNORECASE | re.DOTALL,
)


def _parse_ixbrl_html(path: Path) -> List[Tuple[str, Dict[str, float]]]:
    out: Dict[str, Dict[str, float]] = {}
    try:
        text = path.read_text(errors="ignore")
    except Exception as exc:
        print(f"      ! iXBRL read failed for {path.name}: {exc}")
        return []

    # contexts
    ctx_end: Dict[str, str] = {}
    for m in _IX_CONTEXT_RE.finditer(text):
        ctx_id = m.group("id")
        end_val = m.group("end") or m.group("inst")
        if ctx_id and end_val:
            ctx_end[ctx_id] = end_val

    for m in _IX_NONFRACTION_RE.finditer(text):
        name = m.group("name")
        ctx = m.group("context")
        raw = re.sub(r"<.*?>", "", m.group("value") or "").strip()
        try:
            val = float(raw.replace(",", ""))
        except Exception:
            continue
        period_end = ctx_end.get(ctx, "unknown")
        canonical: Optional[str] = None
        tag = _localname(name)
        for key, aliases in _TAG_SYNONYMS.items():
            if tag in aliases:
                canonical = key
                break
        if not canonical:
            continue
        bucket = out.setdefault(period_end, {})
        if canonical not in bucket:
            bucket[canonical] = val

    return [(k, v) for k, v in out.items()]


def _parse_pdf_ocr(path: Path) -> List[Tuple[str, Dict[str, float]]]:
    """
    Extracts financial facts from a PDF using an OCR pipeline.
    This is a fallback for when XBRL/iXBRL parsing fails.
    """
    print(f"      - Attempting OCR for {path.name}...")
    # TODO: Implement the OCR pipeline
    # 1. Convert PDF to images (e.g., using pdftoppm or a Python library)
    # 2. Run Tesseract on each image
    # 3. Parse the Tesseract output (TSV) to extract financial facts
    # 4. Return a list of (period_end, facts) tuples
    return []


# -----------------
# Main collation
# -----------------

def run() -> None:
    load_dotenv(_resolve_path(".env"))

    config = load_config()
    criteria = config.search_criteria
    tech_config = config.technical_config

    fin_cfg = tech_config.get("financials_scan", {})
    if not fin_cfg or not fin_cfg.get("enabled", False):
        print("[collation] financials_scan disabled in config. Skipping.")
        return

    search_name = criteria.get("name", "default_search")
    base_path = _resolve_path(tech_config.get("storage", {}).get("base_path", "project_data/default"))
    base_path = base_path.with_name(base_path.name.format(name=search_name))

    json_out_root = base_path / "json" / "financials"
    _ensure_dir(json_out_root)

    # Optional download step
    download_documents = bool(fin_cfg.get("download_documents", False))
    window_days = int(fin_cfg.get("window_days", 365))
    parse_ixbrl = bool(fin_cfg.get("parse_ixbrl", True))
    parse_xbrl = bool(fin_cfg.get("parse_xbrl", True))

    companies_df = _find_and_filter_companies(criteria, tech_config)
    if companies_df.empty:
        print("  - No companies matched the filter criteria. Nothing to do.")
        return

    session: Optional[requests.Session] = None
    rate_limit = tech_config.get("api_enrichment", {}).get("request_rate_per_minute", 60)
    interval = max(0.0, 60.0 / float(rate_limit))

    if download_documents:
        api_key = os.getenv("COMPANIES_HOUSE_API_KEY")
        if not api_key or api_key == "YOUR_API_KEY_HERE":
            print("! API key not found. Please set COMPANIES_HOUSE_API_KEY in .env or disable download_documents.")
            sys.exit(1)
        session = requests.Session()
        session.auth = (api_key, "")

    since = _dt_utc_now() - timedelta(days=window_days)
    print(f"[collation] financials: window_days={window_days}, download_documents={download_documents}")

    for idx, row in companies_df.iterrows():
        company_number = row["CompanyNumber"]
        company_name = row["CompanyName"]
        print(f"  ({idx + 1}/{len(companies_df)}) {company_number} — {company_name}")

        fin_root = base_path / "financials" / company_number
        docs_dir = fin_root / "documents"
        _ensure_dir(docs_dir)

        if session is not None:
            try:
                history = _fetch_filing_history(session, company_number)
                items = history.get("items", []) or []
                recent = _filter_since(items, since)
                for it in recent:
                    out_path = _download_document(session, it, docs_dir)
                    if out_path:
                        print(f"    - downloaded: {out_path.name}")
                    time.sleep(interval)
            except requests.RequestException as exc:
                print(f"    ! filing-history failed: {exc}")

        # Parse documents present
        periods: Dict[str, Dict[str, float]] = {}
        parsed_sources: List[str] = []
        doc_paths = list(docs_dir.glob("**/*"))
        for doc_path in doc_paths:
            if doc_path.suffix.lower() == ".xml" and parse_xbrl:
                for period_end, facts in _parse_xbrl_xml(doc_path):
                    bucket = periods.setdefault(period_end, {})
                    for k, v in facts.items():
                        bucket.setdefault(k, v)
                parsed_sources.append("xbrl")
            elif doc_path.suffix.lower() == ".html" and parse_ixbrl:
                for period_end, facts in _parse_ixbrl_html(doc_path):
                    bucket = periods.setdefault(period_end, {})
                    for k, v in facts.items():
                        bucket.setdefault(k, v)
                parsed_sources.append("ixbrl")

        if not parsed_sources:
            for doc_path in doc_paths:
                if doc_path.suffix.lower() == ".pdf":
                    ocr_results = _parse_pdf_ocr(doc_path)
                    if ocr_results:
                        for period_end, facts in ocr_results:
                            bucket = periods.setdefault(period_end, {})
                            for k, v in facts.items():
                                bucket.setdefault(k, v)
                        parsed_sources.append("ocr")

        # Build output JSON structure (foreign key to company)
        out_obj: Dict[str, Any] = {
            "company_fk": company_number,
            "company_number": company_number,
            "company_name": company_name,
            "periods": [
                {
                    "period_end": k,
                    "facts": v,
                }
                for k, v in sorted(periods.items(), key=lambda kv: kv[0] or "")
                if k and _iso_date_to_dt(k) and _iso_date_to_dt(k) >= since
            ],
            "sources": sorted(set(parsed_sources)),
        }

        out_path = json_out_root / f"{company_number}.json"
        with out_path.open("w", encoding="utf-8") as fh:
            json.dump(out_obj, fh, indent=2)
        print(f"    - financial JSON → {out_path.relative_to(_resolve_path(''))}")


def main() -> None:
    run()


if __name__ == "__main__":
    main()
