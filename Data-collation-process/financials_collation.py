from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import subprocess
import tempfile
import shutil
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

    def _cmd_exists(cmd: str) -> bool:
        return shutil.which(cmd) is not None

    def _run_cmd(args: List[str]) -> subprocess.CompletedProcess:
        return subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)

    def _pdf_to_images(pdf_path: Path, workdir: Path) -> List[Path]:
        images: List[Path] = []
        prefix = workdir / "page"
        # Prefer pdftoppm for high-quality rasterisation
        if _cmd_exists("pdftoppm"):
            proc = _run_cmd(["pdftoppm", "-q", "-r", "300", "-png", str(pdf_path), str(prefix)])
            if proc.returncode != 0:
                print(f"        ! pdftoppm failed: {proc.stderr.strip()[:200]}")
            else:
                images = sorted(workdir.glob("page-*.png"))
        # Fallback to Ghostscript
        if not images and _cmd_exists("gs"):
            outdir = workdir / "gs"
            outdir.mkdir(exist_ok=True)
            out_tmpl = str(outdir / "page-%03d.png")
            proc = _run_cmd([
                "gs", "-dBATCH", "-dNOPAUSE", "-sDEVICE=png16m", "-r300",
                f"-sOutputFile={out_tmpl}", str(pdf_path),
            ])
            if proc.returncode != 0:
                print(f"        ! gs rasterisation failed: {proc.stderr.strip()[:200]}")
            else:
                images = sorted(outdir.glob("page-*.png"))
        return images

    def _tesseract_lines(img_path: Path) -> List[str]:
        if not _cmd_exists("tesseract"):
            print("        ! tesseract binary not found in PATH. Install tesseract to enable OCR.")
            return []
        # Use stdout to avoid creating temp files; PSM 6 treats input as a block of text
        proc = _run_cmd([
            "tesseract", str(img_path), "stdout", "-l", "eng", "--oem", "1", "--psm", "6"
        ])
        if proc.returncode != 0:
            # Some tesseract builds print to stderr even on success; only treat true non-zero as failure
            print(f"        ! tesseract failed on {img_path.name}: {proc.stderr.strip()[:200]}")
            return []
        text = proc.stdout or ""
        # Split into non-empty lines for easier parsing
        return [ln.strip() for ln in text.splitlines() if ln.strip()]

    def _norm_number_token(tok: str) -> Optional[float]:
        # Remove currency and whitespace within numbers
        raw = tok.strip()
        raw = raw.replace("£", "").replace("GBP", "").replace("€", "").replace("$", "")
        # Handle common thousand separators and spaces
        raw = raw.replace(",", "").replace("\u00A0", " ")
        # Parentheses indicate negative in many financial statements
        neg = False
        if raw.startswith("(") and raw.endswith(")"):
            neg = True
            raw = raw[1:-1]
        # Strip stray trailing punctuation
        raw = raw.strip().rstrip(".:;")
        # If the token still contains non-number chars, skip
        if not re.match(r"^[+-]?(?:\d+|\d*\.\d+)$", raw):
            return None
        try:
            val = float(raw)
            return -val if neg else val
        except Exception:
            return None

    TEXT_SYNONYMS: Dict[str, Tuple[str, ...]] = {
        # Lowercased keyword variants seen in UK accounts
        "turnover": ("turnover", "revenue", "sales", "income"),
        "profit_loss": (
            "profit for the year",
            "profit for the period",
            "loss for the year",
            "loss for the period",
            "profit and loss",
            "profit/(loss)",
            "profit (loss)",
            "net income",
            "net loss",
            "profit",
            "loss",
        ),
        "operating_profit": ("operating profit", "operating loss"),
        "cash": ("cash", "cash at bank", "cash and cash equivalents"),
        "total_assets": ("total assets", "assets"),
        "net_assets": ("net assets", "net assets/liabilities"),
        "total_liabilities": ("total liabilities", "liabilities"),
    }

    def _map_key(line_lc: str) -> Optional[str]:
        for key, phrases in TEXT_SYNONYMS.items():
            for ph in phrases:
                if ph in line_lc:
                    return key
        return None

    DATE_PATTERNS = [
        # 31 December 2023, 1 Jan 2024
        re.compile(r"(?P<d>\d{1,2})\s+(?P<m>[A-Za-z]{3,9})\s+(?P<y>\d{4})", re.IGNORECASE),
        # 31/12/2023 or 31-12-2023
        re.compile(r"(?P<d>\d{1,2})[\-/](?P<m>\d{1,2})[\-/](?P<y>\d{2,4})"),
        # 2023-12-31
        re.compile(r"(?P<y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})"),
    ]

    MONTHS: Dict[str, int] = {
        m.lower(): i for i, m in enumerate(
            ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"], start=1
        )
    }

    def _to_iso_date(groups: Dict[str, str]) -> Optional[str]:
        try:
            y = int(groups["y"]) if len(groups["y"]) == 4 else 2000 + int(groups["y"])
            m_val = groups["m"]
            if m_val.isdigit():
                m = int(m_val)
            else:
                m = MONTHS.get(m_val.lower(), 0)
            d = int(groups["d"])
            dt = datetime(y, m, d, tzinfo=timezone.utc)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            return None

    def _find_period_end(text: str) -> Optional[str]:
        # Look for phrases like "year/period ended 31 December 2023"
        anchor = re.compile(r"(year|period|months?|quarter)\s+(ended|ending|to)\s+", re.IGNORECASE)
        for ln in text.splitlines():
            if not anchor.search(ln):
                continue
            # Try date patterns on the tail of the line
            tail = ln
            for pat in DATE_PATTERNS:
                m = pat.search(tail)
                if m:
                    iso = _to_iso_date(m.groupdict())
                    if iso:
                        return iso
        # Fallback: scan whole text for a plausible date and trust the most recent-looking
        for pat in DATE_PATTERNS:
            for m in pat.finditer(text):
                iso = _to_iso_date(m.groupdict())
                if iso:
                    return iso
        return None

    # Pipeline: PDF → images → tesseract → text lines
    with tempfile.TemporaryDirectory(prefix="ocr_") as tmpdir:
        tmp = Path(tmpdir)
        images = _pdf_to_images(path, tmp)
        if not images:
            # As a very last resort, try tesseract directly on PDF (may fail on many builds)
            if _cmd_exists("tesseract"):
                lines = _tesseract_lines(path)
            else:
                lines = []
        else:
            lines: List[str] = []
            for img in images:
                lines.extend(_tesseract_lines(img))

    if not lines:
        print("        ! OCR produced no text. Skipping.")
        return []

    full_text = "\n".join(lines)
    period_end = _find_period_end(full_text)

    # Extract figures by scanning lines for keywords and right-most numbers
    facts: Dict[str, float] = {}
    for ln in lines:
        ll = ln.lower()
        key = _map_key(ll)
        if not key:
            continue
        # Find all numeric-looking tokens on the line and use the last as the current-period figure
        tokens = re.findall(r"[\(\-]?\d{1,3}(?:[\s,]?\d{3})*(?:\.\d+)?\)?", ln)
        if not tokens:
            continue
        last_val: Optional[float] = None
        for tok in tokens:
            v = _norm_number_token(tok.replace(" ", ""))
            if v is not None:
                last_val = v
        if last_val is None:
            continue
        # Heuristic: if the line emphasises 'loss' without 'profit', treat as negative if positive
        if ("loss" in ll) and ("profit" not in ll) and last_val > 0:
            last_val = -last_val
        facts.setdefault(key, last_val)

    if not facts:
        print("        ! No recognizable financial facts found in OCR text.")
        return []

    return [(
        period_end or "",
        facts,
    )]


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
    # Output naming configuration
    out_cfg = fin_cfg.get("output_naming", {}) or {}
    folder_by = (out_cfg.get("folder_by") or "company_name").lower()
    folder_template = out_cfg.get("folder_template")
    aggregated_template = out_cfg.get("aggregated_template") or "Historical-financial-performance-{company_name}.json"
    yearly_template = out_cfg.get("yearly_template") or "{year}, {company_name}.json"
    # Window config: env FINANCIAL_YEARS > config.window_years > config.window_days
    env_years = os.getenv("FINANCIAL_YEARS")
    window_years_cfg = fin_cfg.get("window_years")
    window_days_cfg = fin_cfg.get("window_days", 365)
    try:
        window_years = int(env_years) if env_years is not None else (int(window_years_cfg) if window_years_cfg is not None else None)
    except Exception:
        window_years = None
    if window_years is not None and window_years > 0:
        window_days = 365 * window_years
    else:
        window_days = int(window_days_cfg)
    parse_ixbrl = bool(fin_cfg.get("parse_ixbrl", True))
    parse_xbrl = bool(fin_cfg.get("parse_xbrl", True))
    cleanup_cfg = (fin_cfg.get("cleanup") or {})
    cleanup_documents = bool(cleanup_cfg.get("remove_documents", False)) or os.getenv("FINANCIAL_CLEANUP", "").strip().lower() in {"1", "true", "yes", "on"}
    cleanup_financials_dir = bool(cleanup_cfg.get("remove_financials_dir", False))
    cleanup_raw_dir = bool(cleanup_cfg.get("remove_raw_dir", False))

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
    if window_years is not None:
        print(f"[collation] financials: window_years={window_years} (~{window_days} days), download_documents={download_documents}, cleanup={cleanup_documents}")
    else:
        print(f"[collation] financials: window_days={window_days}, download_documents={download_documents}, cleanup={cleanup_documents}")

    # Optional: restrict output to specific fields via config or env var
    # Precedence: FINANCIAL_FIELDS env (comma-separated) > config.financials_scan.fields (list or comma-separated string)
    desired_fields_env = os.getenv("FINANCIAL_FIELDS", "").strip()
    desired_fields_cfg = fin_cfg.get("fields")
    desired_fields: Optional[set] = None
    if desired_fields_env:
        desired_fields = {f.strip() for f in desired_fields_env.split(",") if f.strip()}
    elif desired_fields_cfg:
        if isinstance(desired_fields_cfg, str):
            desired_fields = {f.strip() for f in desired_fields_cfg.split(",") if f.strip()}
        elif isinstance(desired_fields_cfg, (list, tuple, set)):
            desired_fields = {str(f).strip() for f in desired_fields_cfg if str(f).strip()}
    if desired_fields:
        print(f"[collation] limiting output to fields: {sorted(desired_fields)}")

    # Load API enriched data (optional merge into aggregated JSON)
    enriched_map: Dict[str, Dict[str, Any]] = {}
    merge_enriched = bool(fin_cfg.get("merge_enriched", False))
    if merge_enriched:
        try:
            enr_path = base_path / "json" / "enriched_data.json"
            if enr_path.exists():
                data = json.loads(enr_path.read_text(encoding="utf-8"))
                if isinstance(data, list):
                    for it in data:
                        cn = str((it or {}).get("company_number") or "")
                        if cn:
                            enriched_map[cn] = it
                print(f"[collation] loaded enriched data for {len(enriched_map)} companies")
            else:
                print("[collation] enriched_data.json not found; skipping merge")
        except Exception as exc:
            print(f"[collation] ! failed to read enriched_data.json: {exc}")

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

        # Optionally filter facts down to desired_fields
        if desired_fields is not None:
            periods = {k: {fk: fv for fk, fv in v.items() if fk in desired_fields} for k, v in periods.items()}

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

        # Build output paths under json/financials/{company_name|company_number}
        # Determine folder token: prefer folder_template using original name+number, else folder_by
        if folder_template:
            raw_folder = folder_template.format(
                company_name=company_name,
                company_number=company_number,
            )
            company_folder_token = _safe_filename(raw_folder)
        else:
            company_folder_token = (
                _safe_filename(company_name) if folder_by == "company_name" else _safe_filename(company_number)
            )
        company_dir = json_out_root / company_folder_token
        _ensure_dir(company_dir)

        def _render_template(tmpl: str, year: Optional[int] = None) -> str:
            # For filenames, keep placeholders tied to actual company name/number (sanitised)
            return _safe_filename(
                tmpl.format(
                    company_name=_safe_filename(company_name),
                    company_number=company_number,
                    year=(year if year is not None else ""),
                )
            )

        out_path = company_dir / _render_template(aggregated_template)
        # Merge enriched API data into aggregated JSON if available
        if merge_enriched and company_number in enriched_map:
            out_obj["enriched"] = enriched_map[company_number]

        with out_path.open("w", encoding="utf-8") as fh:
            json.dump(out_obj, fh, indent=2)
        print(f"    - financial JSON → {out_path.relative_to(_resolve_path(''))}")

        # Also write per-year JSON files (latest period per year within window) into same company folder
        year_latest: Dict[int, Tuple[str, Dict[str, float]]] = {}
        for k, v in periods.items():
            if not k or not _iso_date_to_dt(k) or _iso_date_to_dt(k) < since:
                continue
            try:
                y = int(k[:4])
            except Exception:
                continue
            cur = year_latest.get(y)
            if cur is None or (k > cur[0]):
                year_latest[y] = (k, v)

        if year_latest:
            for y, (k, v) in sorted(year_latest.items()):
                year_obj: Dict[str, Any] = {
                    "company_fk": company_number,
                    "company_number": company_number,
                    "company_name": company_name,
                    "year": y,
                    "period_end": k,
                    "facts": v,
                    "sources": sorted(set(parsed_sources)),
                }
                y_path = company_dir / _render_template(yearly_template, year=y)
                with y_path.open("w", encoding="utf-8") as fh:
                    json.dump(year_obj, fh, indent=2)
                print(f"      - year JSON → {y_path.relative_to(_resolve_path(''))}")

        # Optional per-company cleanup: remove downloaded documents to save space
        if cleanup_documents and docs_dir.exists():
            try:
                import shutil as _shutil
                _shutil.rmtree(docs_dir, ignore_errors=True)
                print("      - cleaned up documents directory")
            except Exception as exc:
                print(f"      ! cleanup failed: {exc}")

    # Optional end-of-run cleanup: remove financials/ and raw/ trees
    if cleanup_financials_dir:
        try:
            import shutil as _shutil
            fin_tree = base_path / "financials"
            if fin_tree.exists():
                _shutil.rmtree(fin_tree, ignore_errors=True)
                print(f"[collation] removed financials directory: {fin_tree.relative_to(_resolve_path(''))}")
        except Exception as exc:
            print(f"[collation] ! failed to remove financials dir: {exc}")
    if cleanup_raw_dir:
        try:
            import shutil as _shutil
            raw_tree = base_path / "raw"
            if raw_tree.exists():
                _shutil.rmtree(raw_tree, ignore_errors=True)
                print(f"[collation] removed raw directory: {raw_tree.relative_to(_resolve_path(''))}")
        except Exception as exc:
            print(f"[collation] ! failed to remove raw dir: {exc}")


def main() -> None:
    run()


if __name__ == "__main__":
    main()
