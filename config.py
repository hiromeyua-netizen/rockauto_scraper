"""
RockAuto scraper – centralized configuration.
Concurrency, timeouts, retries, proxy, bandwidth targets.
"""
import os
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "rockauto.db"
CATALOG_PRODUCT_DB_PATH = BASE_DIR / "catalog_product.db"  # catalog + product URLs (id, parentId, no, url, isCatalog, scrapeStatus, createdAt)
VEHICLES_CSV = BASE_DIR / "vehicles.csv"  # optional: year,make,model[,engine_slug]
MAKE_YEAR_MODEL_CSV = BASE_DIR / "make_year_model.csv"  # make,year,model,url pairs from catalog tree; used to drive Phase 2 scrape

# Concurrency (safe on 4GB VPS: 10–15 contexts)
MAX_CONCURRENT_CONTEXTS = int(os.getenv("ROCKAUTO_MAX_CONTEXTS", "10"))
CONTEXT_MAX_PAGES = int(os.getenv("ROCKAUTO_CONTEXT_MAX_PAGES", "150"))  # recycle after N pages
CONTEXT_MAX_MINUTES = int(os.getenv("ROCKAUTO_CONTEXT_MAX_MINUTES", "25"))  # recycle after N minutes

# Timeouts (ms)
PAGE_LOAD_TIMEOUT = int(os.getenv("ROCKAUTO_PAGE_TIMEOUT", "45000"))
NAVIGATION_TIMEOUT = int(os.getenv("ROCKAUTO_NAV_TIMEOUT", "30000"))
SOFT_BLOCK_WAIT_MS = int(os.getenv("ROCKAUTO_SOFT_BLOCK_WAIT", "60000"))  # 1 min before retry

# Retries
MAX_RETRIES = int(os.getenv("ROCKAUTO_MAX_RETRIES", "3"))
RETRY_DELAY_SEC = float(os.getenv("ROCKAUTO_RETRY_DELAY", "5.0"))

# Soft block detection (keep specific to avoid false positives on normal catalog pages)
SOFT_BLOCK_EMPTY_PAGES_THRESHOLD = int(os.getenv("ROCKAUTO_SOFT_BLOCK_EMPTY", "5"))  # N empty in a row
# Only RockAuto's known loading image; avoid generic [class*='loading'] which matches normal UI
LOADING_SELECTOR = "img[src*='interstitial_loading']"
# Phrases that appear on actual block/error pages only (avoid generic words that appear in catalog text)
# e.g. do not use "blocked" – it appears in product text (blocked drain, etc.)
EMPTY_RESULT_INDICATORS = [
    "access denied",
    "temporarily unavailable",
    "please try again later",
    "no parts found",
    "no results found",
]

# Bandwidth
BANDWIDTH_LOG_EVERY_N_PAGES = 100

# Browser visibility (default True for Ubuntu/server; set ROCKAUTO_HEADLESS=0 to show window)
HEADLESS = os.getenv("ROCKAUTO_HEADLESS", "1").strip().lower() in ("1", "true", "yes")

# Keep browser open after run until user presses Enter (default True when visible; set ROCKAUTO_KEEP_BROWSER_OPEN=0 to close immediately)
_env_keep = os.getenv("ROCKAUTO_KEEP_BROWSER_OPEN", "").strip().lower()
KEEP_BROWSER_OPEN = _env_keep in ("1", "true", "yes") if _env_keep else (not HEADLESS)

# Bright Data (from .env at repo root or here)
def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()

PROXY_SERVER = _env("BRIGHTDATA_PROXY_SERVER", _env("PROXY_SERVER", "brd.superproxy.io:33335"))
PROXY_USER = _env("BRIGHTDATA_PROXY_USER", _env("PROXY_USER", ""))
PROXY_PASS = _env("BRIGHTDATA_PROXY_PASS", _env("PROXY_PASS", ""))

def get_proxy_url() -> str | None:
    if not (PROXY_USER and PROXY_PASS):
        return None
    return f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_SERVER}"

# Base URL
BASE_URL = "https://www.rockauto.com"
CATALOG_URL = f"{BASE_URL}/en/catalog/"
