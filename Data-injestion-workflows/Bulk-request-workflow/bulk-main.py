from __future__ import annotations

import shutil
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen
from zipfile import ZIP_DEFLATED, ZipFile

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from ingestion_config.loader import load_config


def run_bulk_download() -> None:
    """Download the configured Companies House bulk files."""
    config = load_config()
    search_name = config.search_criteria.get("name", "default_search")
    bulk_config = config.technical_config.get("bulk_download", {})
    storage_config = config.technical_config.get("storage", {})

    print(f"[bulk] starting download for search: {search_name}")

    base_path = _resolve_path(storage_config.get("base_path", "project_data/default"))
    raw_root = base_path.with_name(base_path.name.format(name=search_name)) / "raw"
    raw_root.mkdir(parents=True, exist_ok=True)

    url_template = bulk_config.get("url_template")
    if not url_template:
        print("  ! No url_template configured for bulk download. Skipping.")
        return

    url = _build_url(bulk_config)
    print(f"  - Downloading from: {url}")

    downloaded_file = _download_to_raw(url, raw_root)

    if downloaded_file:
        print(f"  - Downloaded to: {downloaded_file}")
    else:
        print("  ! Bulk download failed.")

    print(f"[bulk] completed download for search: {search_name}")


def _resolve_path(path_value) -> Path:
    path = Path(path_value)
    return path if path.is_absolute() else ROOT / path


def _build_url(bulk_config: dict) -> str:
    template = bulk_config["url_template"]
    date_override = bulk_config.get("date_override")
    if date_override:
        return template.replace("{date}", str(date_override))

    date_format = bulk_config.get("date_format", "%Y-%m-%d")
    dt = datetime.now(timezone.utc) - timedelta(days=1)
    return template.replace("{date}", dt.strftime(date_format))


def _download_to_raw(url: str, raw_root: Path) -> Path | None:
    try:
        filename = Path(urlparse(url).path).name
        destination = raw_root / "company_profile" / filename
        destination.parent.mkdir(parents=True, exist_ok=True)

        if destination.exists():
            print(f"  - File already exists: {filename}")
            return destination

        print(f"  - Downloading to {destination}")
        with urlopen(url) as response, destination.open("wb") as handle:
            shutil.copyfileobj(response, handle)
        return destination
    except Exception as exc:
        print(f"  ! Download failed: {exc}")
        return None


def main() -> None:
    run_bulk_download()


if __name__ == "__main__":
    main()