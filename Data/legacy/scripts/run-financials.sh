#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: run-financials.sh [--fields KEY1,KEY2,...] [--years N] [--cleanup] [--no-install]

Options:
  --fields       Comma-separated list of financial keys to include in output
                 (e.g. turnover,profit_loss,operating_profit,cash,total_assets)
  --years        Limit scan window to last N years (overrides config)
  --cleanup      Delete downloaded documents after collation
  --no-install   Skip 'pip3 install -r requirements.txt'
  -h, --help     Show this help and exit

Notes:
  - Fields filter applies to the 'facts' section per period in the output JSON.
  - The set of keys depends on what the parser extracts (xbrl/ixbrl/ocr).
  - Ensure Tesseract and Poppler/Ghostscript are installed for OCR PDFs.
USAGE
}

FIELDS=""
YEARS=""
CLEANUP=0
DO_INSTALL=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --fields)
      shift
      FIELDS=${1:-}
      [[ -z "$FIELDS" ]] && { echo "--fields requires a value" >&2; exit 2; }
      ;;
    --no-install)
      DO_INSTALL=0
      ;;
    --years)
      shift
      YEARS=${1:-}
      if [[ -z "$YEARS" || ! "$YEARS" =~ ^[0-9]+$ ]]; then
        echo "--years requires a positive integer" >&2; exit 2
      fi
      ;;
    --cleanup)
      CLEANUP=1
      ;;
    -h|--help)
      usage; exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage; exit 2
      ;;
  esac
  shift
done

if [[ $DO_INSTALL -eq 1 ]]; then
  echo "Installing dependencies from requirements.txt..."
  pip3 install -r requirements.txt
fi

if [[ -n "$FIELDS" ]]; then
  export FINANCIAL_FIELDS="$FIELDS"
  echo "Filtering output to fields: $FINANCIAL_FIELDS"
fi

if [[ -n "$YEARS" ]]; then
  export FINANCIAL_YEARS="$YEARS"
  echo "Limiting scan window to last $YEARS year(s)"
fi

if [[ $CLEANUP -eq 1 ]]; then
  export FINANCIAL_CLEANUP=1
  echo "Cleanup of downloaded documents is enabled"
fi

echo "Running financials collation with python3..."
python3 Data-collation-process/financials_collation.py

echo "Financials collation complete. Outputs under project_data/{name}/json/financials/"
