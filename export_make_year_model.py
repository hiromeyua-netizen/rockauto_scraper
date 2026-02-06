"""
Step 1 only: discover all make/year/model from RockAuto catalog and export to make_year_model.csv.
Run this alone to refresh the CSV without scraping products.

Usage:
  python export_make_year_model.py
  python export_make_year_model.py --keep-browser-open
  python export_make_year_model.py --no-keep-browser-open
"""
import argparse
import asyncio
import signal
import sys
from pathlib import Path

# Ensure project root and .env are loaded (same as scraper.py)
_rockauto_root = Path(__file__).resolve().parent
_project_root = _rockauto_root.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
try:
    from dotenv import load_dotenv
    load_dotenv(_project_root / ".env")
    load_dotenv(_rockauto_root / ".env")
except ImportError:
    pass

from scraper import (
    run_export_make_year_model,
    _set_shutdown,
    logger,
    KEEP_BROWSER_OPEN,
)


def main():
    ap = argparse.ArgumentParser(description="Export make/year/model pairs to make_year_model.csv")
    ap.add_argument("--keep-browser-open", action="store_true", help="Wait for Enter before closing browser")
    ap.add_argument("--no-keep-browser-open", action="store_true", dest="no_keep_browser_open", help="Close browser immediately when done")
    args = ap.parse_args()

    keep_browser_open = KEEP_BROWSER_OPEN
    if args.keep_browser_open:
        keep_browser_open = True
    if getattr(args, "no_keep_browser_open", False):
        keep_browser_open = False

    signal.signal(signal.SIGTERM, _set_shutdown)
    signal.signal(signal.SIGINT, _set_shutdown)

    n = asyncio.run(run_export_make_year_model(keep_browser_open=keep_browser_open))
    logger.info("Done. Exported %d rows to make_year_model.csv", n)
    return 0 if n >= 0 else 1


if __name__ == "__main__":
    sys.exit(main())
