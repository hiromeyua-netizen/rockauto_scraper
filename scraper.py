"""
RockAuto async scraper – Playwright, bandwidth optimization, checkpoint/resume,
soft-block handling, context recycling. Output: SQLite (rockauto.db) then export to CSV.
"""
import argparse
import asyncio
import csv
import logging
import re
import signal
import sys
from datetime import datetime
from pathlib import Path
import json

from playwright.async_api import async_playwright, Page, Route, Request, BrowserContext, Browser
from tqdm import tqdm

# Add parent for .env load if present
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

try:
    from rockauto.config import (
        DB_PATH,
        VEHICLES_CSV,
        MAKE_YEAR_MODEL_CSV,
        MAX_CONCURRENT_CONTEXTS,
        CONTEXT_MAX_PAGES,
        CONTEXT_MAX_MINUTES,
        PAGE_LOAD_TIMEOUT,
        NAVIGATION_TIMEOUT,
        SOFT_BLOCK_WAIT_MS,
        MAX_RETRIES,
        RETRY_DELAY_SEC,
        SOFT_BLOCK_EMPTY_PAGES_THRESHOLD,
        LOADING_SELECTOR,
        EMPTY_RESULT_INDICATORS,
        BANDWIDTH_LOG_EVERY_N_PAGES,
        get_proxy_url,
        CATALOG_URL,
        BASE_URL,
        HEADLESS,
        KEEP_BROWSER_OPEN,
    )
    from rockauto.models import (
        get_connection,
        init_db,
        get_checkpoint,
        save_checkpoint,
        delete_rockauto_by_vehicle,
        insert_rockauto,
        insert_product_urls,
        fetch_product_urls,
        mark_product_urls_in_progress,
        mark_product_url_done,
        mark_product_url_error,
        init_catalog_product_db,
        get_catalog_product_connection,
        insert_first_level_catalogs,
        insert_child_catalogs,
        insert_child_products,
        get_catalog_by_url,
        fetch_first_level_catalogs,
        fetch_year_level_catalogs,
        fetch_catalogs_by_path_segment_count,
        CATALOG_PRODUCT_ROOT_PARENT_ID,
    )
except ImportError:
    from config import (
        DB_PATH,
        VEHICLES_CSV,
        MAKE_YEAR_MODEL_CSV,
        MAX_CONCURRENT_CONTEXTS,
        CONTEXT_MAX_PAGES,
        CONTEXT_MAX_MINUTES,
        PAGE_LOAD_TIMEOUT,
        NAVIGATION_TIMEOUT,
        SOFT_BLOCK_WAIT_MS,
        MAX_RETRIES,
        RETRY_DELAY_SEC,
        SOFT_BLOCK_EMPTY_PAGES_THRESHOLD,
        LOADING_SELECTOR,
        EMPTY_RESULT_INDICATORS,
        BANDWIDTH_LOG_EVERY_N_PAGES,
        get_proxy_url,
        CATALOG_URL,
        BASE_URL,
        HEADLESS,
        KEEP_BROWSER_OPEN,
    )
    from models import (
        get_connection,
        init_db,
        get_checkpoint,
        save_checkpoint,
        delete_rockauto_by_vehicle,
        insert_rockauto,
        insert_product_urls,
        fetch_product_urls,
        mark_product_urls_in_progress,
        mark_product_url_done,
        mark_product_url_error,
        init_catalog_product_db,
        get_catalog_product_connection,
        insert_first_level_catalogs,
        insert_child_catalogs,
        insert_child_products,
        get_catalog_by_url,
        fetch_first_level_catalogs,
        fetch_year_level_catalogs,
        fetch_catalogs_by_path_segment_count,
        CATALOG_PRODUCT_ROOT_PARENT_ID,
    )

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("rockauto")

# Stealth: optional (apply to each new page for RockAuto anti-bot)
try:
    from playwright_stealth import StealthConfig
    from playwright_stealth.stealth import stealth_async
    STEALTH_AVAILABLE = True
except ImportError:
    STEALTH_AVAILABLE = False
    stealth_async = None

_shutdown = False


async def _wait_before_close(keep_browser_open: bool):
    """If keep_browser_open, wait for Enter so the user can inspect the browser."""
    if not keep_browser_open:
        return
    logger.info("Scrape finished. Press Enter to close the browser...")
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, lambda: input("Press Enter to close the browser... "))


def _set_shutdown(*_):
    global _shutdown
    _shutdown = True
    logger.info("SIGTERM/SIGINT received; saving checkpoint and exiting.")


# --------------- Bandwidth tracking & route abort ---------------
def _create_bandwidth_tracker():
    data = {"bytes": 0, "pages": 0}

    async def on_route(route: Route, request: Request):
        resource_type = request.resource_type
        if resource_type in ("image", "imageset", "font", "stylesheet", "media"):
            await route.abort()
            return
        try:
            response = await route.fetch()
            body = await response.body()
            data["bytes"] += len(body)
            await route.fulfill(response=response, body=body)
        except Exception:
            await route.continue_()

    def add_page():
        data["pages"] += 1
        if data["pages"] % BANDWIDTH_LOG_EVERY_N_PAGES == 0:
            kb = data["bytes"] / 1024
            logger.info(
                "Bandwidth (last %d pages): %.1f KB total (~%.1f KB/page)",
                data["pages"], kb, kb / data["pages"] if data["pages"] else 0,
            )

    return on_route, add_page, data


# --------------- Soft block detection ---------------
async def _wait_for_no_loading(page: Page, timeout_ms: int = 15000) -> bool:
    """Wait for RockAuto loading interstitial to disappear. Returns True if it did."""
    try:
        await page.wait_for_selector(LOADING_SELECTOR, state="hidden", timeout=timeout_ms)
        return True
    except Exception:
        pass
    return False


async def _wait_for_catalog_ready(page: Page, timeout_ms: int = 20000) -> None:
    """After navigation: wait for loading to disappear so catalog content can be read."""
    await _wait_for_no_loading(page, timeout_ms)
    await asyncio.sleep(1)


async def _is_soft_blocked(page: Page) -> bool:
    """
    True only when page is clearly a block/error page. Avoid false positives:
    - If the page has any catalog links, we're on a normal catalog page → not blocked.
    - Only treat as blocked when block phrase appears AND no catalog links (real block/error page).
    """
    try:
        catalog_links = await page.locator('a[href*="/en/catalog/"]').count()
        if catalog_links > 0:
            return False  # Normal catalog page
    except Exception:
        pass
    content = await page.content()
    lower = content.lower()
    for phrase in EMPTY_RESULT_INDICATORS:
        if phrase in lower:
            return True
    return False


# --------------- Context with recycling ---------------
class ContextWrapper:
    def __init__(self, context: BrowserContext, created_at: float, max_pages: int, max_minutes: int):
        self.context = context
        self.created_at = created_at
        self.max_pages = max_pages
        self.max_minutes = max_minutes
        self.pages_done = 0

    def should_recycle(self) -> bool:
        import time
        if self.pages_done >= self.max_pages:
            return True
        if (time.time() - self.created_at) / 60 >= self.max_minutes:
            return True
        return False


# --------------- Navigation & extraction ---------------
def _make_catalog_url(make_slug: str, year: str | None = None, model_slug: str | None = None, engine_slug: str | None = None) -> str:
    parts = [make_slug]
    if year:
        parts.append(year)
    if model_slug:
        parts.append(model_slug)
    if engine_slug:
        # engine_slug may be "2.5l+l4,3445372" (slug,id)
        parts.append(engine_slug)
    return f"{CATALOG_URL}{','.join(parts)}"


async def _get_makes_from_catalog(page: Page) -> list[tuple[str, str]]:
    """Returns [(slug, display_name), ...] from main catalog."""
    await page.goto(CATALOG_URL, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
    await asyncio.sleep(1.5)
    links = await page.locator("a[href*='/en/catalog/']").evaluate_all(
        """els => els
            .map(a => ({ href: a.href, text: a.textContent?.trim() || '' }))
            .filter(x => x.href && x.href.match(/\\/en\\/catalog\\/[a-z0-9+_-]+$/) && x.text)
            .map(x => ({ slug: x.href.split('/').pop(), name: x.text }))
        """
    )
    seen = set()
    out = []
    for item in links:
        slug = (item.get("slug") or "").strip()
        name = (item.get("name") or "").strip()
        if slug and slug not in seen and len(slug) > 1:
            seen.add(slug)
            out.append((slug, name))
    return out


async def _get_years_for_make(page: Page, make_slug: str) -> list[str]:
    url = _make_catalog_url(make_slug)
    for attempt in range(MAX_RETRIES):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
            break
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                raise
            logger.warning("_get_years_for_make %s attempt %s: %s", url, attempt + 1, e)
            await asyncio.sleep(RETRY_DELAY_SEC)
    await asyncio.sleep(1)
    links = await page.locator("a[href*='/en/catalog/']").evaluate_all(
        """els => els
            .map(a => a.href)
            .filter(h => h && h.split(',').length === 2)
            .map(h => {
                const parts = h.split('/').pop().split(',');
                return parts[parts.length - 1];
            })
            .filter(y => /^\\d{4}$/.test(y))
        """
    )
    return list(dict.fromkeys(links))


async def _expand_first_level_catalogs(page: Page, conn, add_page_fn=None) -> int:
    """
    Visit each first-level catalog (make page), extract child year links,
    save as child catalogs in catalog_product.db (parentId, no=1-1,1-2,..., url).
    Returns total child catalogs inserted.
    """
    if add_page_fn is None:
        add_page_fn = lambda: None
    rows = fetch_first_level_catalogs(conn)
    total_inserted = 0
    for row in rows:
        if _shutdown:
            break
        url = row["url"] or ""
        make_slug = url.rstrip("/").split("/")[-1] or ""
        if not make_slug:
            continue
        try:
            years = await _get_years_for_make(page, make_slug)
            add_page_fn()
            if not years:
                continue
            child_urls = [f"{CATALOG_URL}{make_slug},{y}" for y in years]
            inserted = insert_child_catalogs(conn, row["id"], str(row["no"]), child_urls)
            conn.commit()
            total_inserted += inserted
            if inserted:
                logger.info("  %s: saved %d child catalogs (years %s)", make_slug, inserted, years[:5] if len(years) > 5 else years)
        except Exception as e:
            logger.warning("Expand catalog %s failed: %s", url, e)
            conn.rollback()
    return total_inserted


async def _expand_second_level_catalogs(page: Page, conn, add_page_fn=None) -> int:
    """
    Visit each year-level catalog (make,year page), extract child model links (e.g. 1000, 1300, 2000, SIMCA),
    save as child catalogs in catalog_product.db (parentId = year row id, no = 1-1-1, 1-1-2, ..., url).
    Returns total child catalogs inserted.
    """
    if add_page_fn is None:
        add_page_fn = lambda: None
    rows = fetch_year_level_catalogs(conn)
    total_inserted = 0
    for row in rows:
        if _shutdown:
            break
        url = (row["url"] or "").split("?")[0]
        if "/en/catalog/" not in url:
            continue
        path = url.split("/en/catalog/")[-1].strip("/")
        parts = path.split(",")
        if len(parts) != 2:
            continue
        make_slug, year = parts[0].strip(), parts[1].strip()
        if not make_slug or not year:
            continue
        try:
            models = await _get_models_for_make_year(page, make_slug, year)
            add_page_fn()
            if not models:
                continue
            child_urls = [f"{CATALOG_URL}{make_slug},{year},{ms}" for ms, _ in models]
            inserted = insert_child_catalogs(conn, row["id"], str(row["no"]), child_urls)
            conn.commit()
            total_inserted += inserted
            if inserted:
                names = [n for _, n in models[:6]]
                logger.info("  %s %s: saved %d child catalogs (%s)", make_slug, year, inserted, names)
        except Exception as e:
            logger.warning("Expand catalog %s failed: %s", url, e)
            conn.rollback()
    return total_inserted


def _catalog_path_parts(url: str) -> list[str]:
    """Return comma-separated path segments from catalog URL."""
    url = (url or "").split("?")[0]
    if "/en/catalog/" not in url:
        return []
    path = url.split("/en/catalog/")[-1].strip("/")
    return [p.strip() for p in path.split(",") if p.strip()]


def _slug_to_label(slug: str) -> str:
    """Turn URL slug into display label (e.g. 982cc+l4 -> 982cc L4)."""
    if not slug:
        return ""
    return slug.replace("+", " ").strip().title()


def _label_to_slug(label: str) -> str:
    """Turn display label into URL slug (e.g. Ford -> ford, F-150 -> f-150)."""
    if not label:
        return ""
    return label.lower().replace(" ", "+").strip()


def _normalize_price(price: str | None) -> str | None:
    """
    Normalize price to [currency][value] (e.g. €4.95, $12.50).
    Strips parentheses and trailing '/Each'; leaves non-price strings (e.g. 'out of stock') unchanged.
    """
    if not price or not (s := price.strip()):
        return None
    # Only normalize strings that look like prices (currency + digit)
    if not re.search(r"[\$€]\s*\d", s):
        return s
    if s.startswith("(") and s.endswith(")"):
        s = s[1:-1].strip()
    s = re.sub(r"/\s*each\s*$", "", s, flags=re.IGNORECASE).strip()
    return s if s else None


def _write_make_year_model_csv(vehicles: list[dict]) -> int:
    """Write all make,year,model pairs from discovered vehicles to MAKE_YEAR_MODEL_CSV. Returns count written. URL is omitted since it can be derived from make, year, model."""
    if not vehicles:
        return 0
    out = []
    for v in vehicles:
        make_slug = v.get("make_slug") or ""
        year = v.get("year") or ""
        model_slug = v.get("model_slug") or ""
        if not (make_slug and year and model_slug):
            continue
        out.append({
            "make": v.get("make") or _slug_to_label(make_slug),
            "year": year,
            "model": v.get("model") or _slug_to_label(model_slug),
        })
    with open(MAKE_YEAR_MODEL_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["make", "year", "model"])
        w.writeheader()
        w.writerows(out)
    return len(out)


def _load_make_year_model_csv() -> list[dict]:
    """Load make,year,model rows from MAKE_YEAR_MODEL_CSV. Returns list of dicts with make, year, model, url (url built from make/year/model if not in CSV)."""
    if not MAKE_YEAR_MODEL_CSV.exists():
        return []
    out = []
    with open(MAKE_YEAR_MODEL_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            make = (row.get("make") or "").strip()
            year = (row.get("year") or "").strip()
            model = (row.get("model") or "").strip()
            if not (make and year and model):
                continue
            url = (row.get("url") or "").strip()
            if not url:
                make_slug = _label_to_slug(make)
                model_slug = _label_to_slug(model)
                url = _make_catalog_url(make_slug, year, model_slug, None)
            out.append({"make": make, "year": year, "model": model, "url": url})
    return out


async def _expand_third_level_catalogs(page: Page, conn, add_page_fn=None) -> int:
    """Visit each model-level catalog (3 segments), get engine links (5 segments), save as children."""
    if add_page_fn is None:
        add_page_fn = lambda: None
    rows = fetch_catalogs_by_path_segment_count(conn, 3)
    total_inserted = 0
    for row in rows:
        if _shutdown:
            break
        url = (row["url"] or "").split("?")[0]
        parts = _catalog_path_parts(url)
        if len(parts) != 3:
            continue
        make_slug, year, model_slug = parts[0], parts[1], parts[2]
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
            add_page_fn()
            await _wait_for_catalog_ready(page)
            engines = await _get_engines_for_vehicle(page, make_slug, year, model_slug)
            if not engines:
                continue
            child_urls = [f"{CATALOG_URL}{make_slug},{year},{model_slug},{eng_suffix}" for eng_suffix, _ in engines]
            inserted = insert_child_catalogs(conn, row["id"], str(row["no"]), child_urls)
            conn.commit()
            total_inserted += inserted
            if inserted:
                logger.info("  %s %s %s: saved %d engine catalogs", make_slug, year, model_slug, inserted)
        except Exception as e:
            logger.warning("Expand catalog %s failed: %s", url, e)
            conn.rollback()
    return total_inserted


async def _expand_fourth_level_catalogs(page: Page, conn, add_page_fn=None) -> int:
    """Visit each engine-level catalog (5 segments), get category links (6 segments), save as children."""
    if add_page_fn is None:
        add_page_fn = lambda: None
    rows = fetch_catalogs_by_path_segment_count(conn, 5)
    total_inserted = 0
    for row in rows:
        if _shutdown:
            break
        url = (row["url"] or "").split("?")[0]
        parts = _catalog_path_parts(url)
        if len(parts) != 5:
            continue
        make_slug, year, model_slug = parts[0], parts[1], parts[2]
        engine_suffix = f"{parts[3]},{parts[4]}"
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
            add_page_fn()
            await _wait_for_catalog_ready(page)
            categories = await _get_category_links_for_vehicle(page, make_slug, year, model_slug, engine_suffix)
            if not categories:
                continue
            child_urls = [f"{CATALOG_URL}{path}" for path, _ in categories]
            inserted = insert_child_catalogs(conn, row["id"], str(row["no"]), child_urls)
            conn.commit()
            total_inserted += inserted
            if inserted:
                logger.info("  %s: saved %d category catalogs", url[-50:], inserted)
        except Exception as e:
            logger.warning("Expand catalog %s failed: %s", url, e)
            conn.rollback()
    return total_inserted


async def _expand_fifth_level_catalogs(page: Page, conn, add_page_fn=None) -> int:
    """Visit each category-level catalog (6 segments), get part-type links (8 segments), save as children."""
    if add_page_fn is None:
        add_page_fn = lambda: None
    rows = fetch_catalogs_by_path_segment_count(conn, 6)
    total_inserted = 0
    for row in rows:
        if _shutdown:
            break
        url = (row["url"] or "").split("?")[0]
        path = url.split("/en/catalog/")[-1].strip("/") if "/en/catalog/" in url else ""
        if path.count(",") != 5:
            continue
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
            add_page_fn()
            await _wait_for_catalog_ready(page)
            part_types = await _get_part_type_links_from_category_page(page, path)
            if not part_types:
                continue
            child_urls = [f"{CATALOG_URL}{pt_path}" for pt_path, _ in part_types]
            inserted = insert_child_catalogs(conn, row["id"], str(row["no"]), child_urls)
            conn.commit()
            total_inserted += inserted
            if inserted:
                logger.info("  %s: saved %d part-type catalogs", path[-40:], inserted)
        except Exception as e:
            logger.warning("Expand catalog %s failed: %s", url, e)
            conn.rollback()
    return total_inserted


async def _expand_to_products(
    page: Page, cp_conn, rockauto_conn, add_page_fn=None
) -> int:
    """
    Visit each part-type catalog (8 segments), scrape all product fields from the listing page
    (no Info page visits), and insert into rockauto.db.
    """
    if add_page_fn is None:
        add_page_fn = lambda: None
    rows = fetch_catalogs_by_path_segment_count(cp_conn, 8)
    total_inserted = 0
    for row in rows:
        if _shutdown:
            break
        url = (row["url"] or "").split("?")[0]
        parts = _catalog_path_parts(url)
        if len(parts) < 8:
            continue
        catalog_page_url = f"{CATALOG_URL}{url}" if not url.startswith("http") else url
        make_label = _slug_to_label(parts[0])
        model_label = _slug_to_label(parts[2])
        engine_label = _slug_to_label(parts[3]) if len(parts) > 4 else ""
        category_label = _slug_to_label(parts[5]) if len(parts) > 6 else ""
        part_type_label = _slug_to_label(parts[6]) if len(parts) > 7 else ""
        try:
            await page.goto(catalog_page_url, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
            add_page_fn()
            await _wait_for_catalog_ready(page)
            products = await _collect_products_from_listing(
                page,
                year=parts[1],
                make=make_label,
                model=model_label,
                engine=engine_label,
                category=category_label,
                part_type_name=part_type_label,
                catalog_page_url=catalog_page_url,
            )
            if not products:
                continue
            insert_rockauto(rockauto_conn, products)
            rockauto_conn.commit()
            total_inserted += len(products)
            logger.info("  %s: saved %d products to rockauto.db", url[-60:], len(products))
        except Exception as e:
            logger.warning("Expand to products %s failed: %s", url, e)
            rockauto_conn.rollback()
    return total_inserted


async def _get_models_for_make_year(page: Page, make_slug: str, year: str) -> list[tuple[str, str]]:
    url = _make_catalog_url(make_slug, year)
    for attempt in range(MAX_RETRIES):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
            break
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                raise
            logger.warning("_get_models_for_make_year %s attempt %s: %s", url, attempt + 1, e)
            await asyncio.sleep(RETRY_DELAY_SEC)
    await asyncio.sleep(1)
    links = await page.locator("a[href*='/en/catalog/']").evaluate_all(
        """els => els
            .map(a => ({ href: a.href, text: a.textContent?.trim() || '' }))
            .filter(x => x.href && x.href.split(',').length >= 3 && x.text)
            .map(x => {
                const slug = x.href.split('/').pop();
                const parts = slug.split(',');
                const modelSlug = parts.length >= 3 ? parts[2] : '';
                return { slug: modelSlug, name: x.text };
            })
            .filter(x => x.slug)
        """
    )
    seen = set()
    out = []
    for item in links:
        slug = (item.get("slug") or "").strip()
        name = (item.get("name") or "").strip()
        if slug and slug not in seen:
            seen.add(slug)
            out.append((slug, name))
    return out


async def _get_engines_for_vehicle(page: Page, make_slug: str, year: str, model_slug: str) -> list[tuple[str, str]]:
    """
    Get engine links (5 segments) from the vehicle page tree.
    Targets a.navlabellink inside treeroot_wrapper[catalog] with href like
    /en/catalog/ford,2021,f-150,2.7l+v6+turbocharged,3447345
    Returns [(engine_suffix, display_name), ...] where engine_suffix is "2.7l+v6+turbocharged,3447345"
    """
    prefix = [make_slug, year, model_slug]
    raw = await _get_tree_links_by_segments(page, 5, prefix)
    out = []
    seen = set()
    for full_suffix, name in raw:
        parts = full_suffix.split(",")
        if len(parts) >= 5:
            engine_suffix = ",".join(parts[3:5])
            if engine_suffix and engine_suffix not in seen:
                seen.add(engine_suffix)
                out.append((engine_suffix, name))
    if not out:
        # Fallback 1: same logic but any navlabellink (case-insensitive prefix match)
        raw_fallback = await page.locator('a.navlabellink[href*="/en/catalog/"]').evaluate_all(
            """(els, ctx) => els
                .map(a => ({ href: a.href, text: a.textContent?.trim() || '' }))
                .filter(x => x.href && x.text)
                .map(x => {
                    const m = x.href.match(/\\/en\\/catalog\\/([^?#]+)/);
                    const suffix = m ? m[1] : '';
                    const parts = suffix.split(',');
                    if (parts.length !== 5) return null;
                    const p0 = (parts[0]||'').toLowerCase();
                    const p1 = (parts[1]||'').toLowerCase();
                    const p2 = (parts[2]||'').toLowerCase();
                    if (p0 !== ctx.make.toLowerCase() || p1 !== ctx.year.toLowerCase() || p2 !== ctx.model.toLowerCase()) return null;
                    return { suffix: parts.slice(3).join(','), name: x.text };
                })
                .filter(x => x && x.suffix)
            """,
            {"make": make_slug, "year": year, "model": model_slug},
        )
        for item in raw_fallback:
            s = (item.get("suffix") or "").strip()
            n = (item.get("name") or "").strip()
            if s and s not in seen:
                seen.add(s)
                out.append((s, n))
    if not out:
        # Fallback 2: any catalog link on page with 5 segments (child trees) – no navlabellink/tree dependency
        raw_fallback2 = await page.locator('a[href*="/en/catalog/"]').evaluate_all(
            """(els, ctx) => els
                .map(a => ({ href: a.href, text: (a.textContent||'').trim() }))
                .filter(x => x.href && x.href.includes('/en/catalog/'))
                .map(x => {
                    const m = x.href.match(/\\/en\\/catalog\\/([^?#]+)/);
                    const suffix = m ? m[1] : '';
                    const parts = suffix.split(',');
                    if (parts.length !== 5) return null;
                    const p0 = (parts[0]||'').toLowerCase();
                    const p1 = (parts[1]||'').toLowerCase();
                    const p2 = (parts[2]||'').toLowerCase();
                    if (p0 !== ctx.make.toLowerCase() || p1 !== ctx.year.toLowerCase() || p2 !== ctx.model.toLowerCase()) return null;
                    return { suffix: parts.slice(3).join(','), name: x.text };
                })
                .filter(x => x && x.suffix)
            """,
            {"make": make_slug, "year": year, "model": model_slug},
        )
        for item in raw_fallback2:
            s = (item.get("suffix") or "").strip()
            n = (item.get("name") or "").strip()
            if s and s not in seen:
                seen.add(s)
                out.append((s, n))
    if not out:
        out.append(("", ""))
    return out


def _get_tree_root_selector() -> str:
    return '[id="treeroot_wrapper[catalog]"]'


async def _get_tree_links_by_segments(
    page: Page,
    expected_segments: int,
    prefix_segments: list[str],
) -> list[tuple[str, str]]:
    """
    Get navlabellink anchors from the catalog tree where the href path has exactly
    expected_segments comma-separated parts, and the first len(prefix_segments) match.
    Returns [(href_suffix, display_name), ...].
    """
    try:
        await page.wait_for_selector(_get_tree_root_selector(), timeout=20000)
    except Exception:
        logger.debug("Tree root not found")
        return []
    try:
        await page.wait_for_selector(LOADING_SELECTOR, state="hidden", timeout=15000)
    except Exception:
        pass
    await asyncio.sleep(2)
    links = await page.locator(f'{_get_tree_root_selector()} a.navlabellink').evaluate_all(
        """(els, ctx) => els
            .map(a => ({ href: a.href, text: a.textContent?.trim() || '' }))
            .filter(x => x.href && x.text && x.href.includes('/en/catalog/'))
            .map(x => {
                const suffix = x.href.replace(/^.*\\/en\\/catalog\\//, '');
                const segments = suffix.split(',');
                return { suffix, segments, name: x.text };
            })
            .filter(x => {
                if (x.segments.length !== ctx.expected) return false;
                for (let i = 0; i < ctx.prefix.length; i++) {
                    const s = (x.segments[i]||'').toLowerCase();
                    const p = (ctx.prefix[i]||'').toLowerCase();
                    if (s !== p) return false;
                }
                return true;
            })
        """,
        {"expected": expected_segments, "prefix": prefix_segments},
    )
    seen = set()
    out = []
    for item in links:
        href = (item.get("suffix") or "").strip()
        name = (item.get("name") or "").strip()
        if href and href not in seen:
            seen.add(href)
            out.append((href, name))
    return out


async def _get_category_links_for_vehicle(
    page: Page, make_slug: str, year: str, model_slug: str, engine_suffix: str
) -> list[tuple[str, str]]:
    """
    Category links (6 segments) on engine page. Targets a.navlabellink with href like
    /en/catalog/ford,2021,f-150,2.7l+v6+turbocharged,3447345,belt+drive
    engine_suffix is like "2.7l+v6+turbocharged,3447345".
    Returns [(full_path, display_name), ...] e.g. ("ford,2021,f-150,2.7l+v6+turbocharged,3447345,belt+drive", "Belt Drive")
    """
    prefix_parts = [make_slug, year, model_slug]
    if engine_suffix:
        prefix_parts.extend(engine_suffix.split(","))
    if len(prefix_parts) < 5:
        return []
    prefix = [p.strip().lower() for p in prefix_parts[:5]]
    raw = await _get_tree_links_by_segments(page, 6, prefix)
    if not raw:
        fb = await page.locator('a.navlabellink[href*="/en/catalog/"]').evaluate_all(
            """(els, ctx) => els
                .map(a => ({ href: a.href, text: a.textContent?.trim() || '' }))
                .filter(x => x.href && x.text && !x.href.includes('/closeouts/'))
                .map(x => {
                    const m = x.href.match(/\\/en\\/catalog\\/([^?#]+)/);
                    const suffix = m ? m[1] : '';
                    const parts = suffix.split(',');
                    if (parts.length !== 6) return null;
                    const pLow = ctx.prefix.map(s => (s||'').toLowerCase());
                    for (let i = 0; i < 5; i++) {
                        if ((parts[i]||'').toLowerCase() !== pLow[i]) return null;
                    }
                    return { suffix, name: x.text };
                })
                .filter(x => x && x.suffix)
            """,
            {"prefix": prefix},
        )
        seen = set()
        raw = []
        for item in fb:
            s = (item.get("suffix") or "").strip()
            n = (item.get("name") or "").strip()
            if s and s not in seen:
                seen.add(s)
                raw.append((s, n))
    return raw


async def _get_part_type_links_from_category_page(
    page: Page, category_path: str
) -> list[tuple[str, str]]:
    """
    Part type links (8 segments) on category page. Targets a.navlabellink (including nimportant)
    with href like /en/catalog/ford,2021,f-150,2.7l+v6+turbocharged,3447345,belt+drive,belt,8900
    category_path is 6-segment suffix e.g. "ford,2021,f-150,2.7l+v6+turbocharged,3447345,belt+drive"
    Returns [(full_path, display_name), ...] e.g. ("...belt+drive,belt,8900", "Belt")
    """
    parts_raw = (category_path or "").split(",")
    if len(parts_raw) < 6:
        return []
    prefix = [p.strip().lower() for p in parts_raw[:6]]
    raw = await _get_tree_links_by_segments(page, 8, prefix)
    if not raw:
        fb = await page.locator('a.navlabellink[href*="/en/catalog/"]').evaluate_all(
            """(els, ctx) => els
                .map(a => ({ href: a.href, text: a.textContent?.trim() || '' }))
                .filter(x => x.href && x.text && !x.href.includes('/closeouts/'))
                .map(x => {
                    const m = x.href.match(/\\/en\\/catalog\\/([^?#]+)/);
                    const suffix = m ? m[1] : '';
                    const parts = suffix.split(',');
                    if (parts.length !== 8) return null;
                    const pLow = ctx.prefix.map(s => (s||'').toLowerCase());
                    for (let i = 0; i < 6; i++) {
                        if ((parts[i]||'').toLowerCase() !== pLow[i]) return null;
                    }
                    return { suffix, name: x.text };
                })
                .filter(x => x && x.suffix)
            """,
            {"prefix": prefix},
        )
        seen = set()
        raw = []
        for item in fb:
            s = (item.get("suffix") or "").strip()
            n = (item.get("name") or "").strip()
            if s and s not in seen:
                seen.add(s)
                raw.append((s, n))
    return raw


async def _collect_products_from_listing(
    page: Page,
    year: str,
    make: str,
    model: str,
    engine: str,
    category: str,
    part_type_name: str,
    catalog_page_url: str = "",
) -> list[dict]:
    """
    Scrape all product fields from a RockAuto part-type listing page (no visit to Info pages).
    Returns list of dicts ready for insert_rockauto: year, make, model, engine, category, part_type,
    manufacturer, part_number, price, description, market_flags, notes, image_url,
    catalog_page_url, product_info_url, sort_group, alternate_oe_numbers.
    """
    try:
        await page.wait_for_selector(".listings-container, tbody.listing-inner", timeout=10000)
    except Exception:
        return []
    await asyncio.sleep(1)
    base_url = (await page.evaluate("() => window.location.origin")) or BASE_URL
    rows = await page.evaluate(
        """(baseUrl) => {
            const out = [];
            const priceInBRegex = /\\([\\$€]?[\\d.,]+(?:\\/[^)]*)?\\)/;
            const tbodies = document.querySelectorAll('tbody.listing-inner, tbody[id^="listingcontainer"]');
            for (const tbody of tbodies) {
                let sortGroup = '';
                let prev = tbody.previousElementSibling;
                if (prev) {
                    const headerDiv = prev.querySelector('.listing-sortgroupheader');
                    if (headerDiv) sortGroup = (headerDiv.textContent || '').trim();
                }
                const trs = tbody.querySelectorAll('tr');
                for (const tr of trs) {
                    const td = tr.querySelector('td.listing-inner-content');
                    if (!td) continue;
                    const mfr = td.querySelector('.listing-final-manufacturer');
                    const pn = td.querySelector('.listing-final-partnumber, [id^="vew_partnumber"]');
                    const desc = td.querySelector('.span-link-underline-remover');
                    const infoLink = td.querySelector('a.ra-btn-moreinfo, a.ra-btn.ra-btn-moreinfo');
                    const footnote = td.querySelector('.listing-footnote-text');
                    const mtf = td.querySelector('.mtf-h-t');
                    const flags = Array.from(td.querySelectorAll('.appflag[data-market], .flag[src*="flag"]'))
                        .map(el => el.getAttribute('data-market') || (el.src && el.src.includes('flag_us') ? 'US' : el.src && el.src.includes('flag_ca') ? 'CA' : el.src && el.src.includes('flag_mx') ? 'MX' : null))
                        .filter(Boolean);
                    const marketFlags = flags.length ? flags.join(',') : '';
                    let notes = footnote ? (footnote.textContent || '').trim() : '';
                    const mtfText = mtf ? (mtf.textContent || '').trim() : '';
                    if (mtfText) notes = (notes + ' ' + mtfText).trim();
                    const oeSpan = td.querySelector('span[title*="Alternate"], span[title*="OE Part"]');
                    const alternateOe = oeSpan ? (oeSpan.textContent || '').trim() : '';
                    let priceEl = tr.querySelector('.listing-price span, .seeopt-price-text, [id^="dprice"] span, .listing-amount-bold span');
                    let price = priceEl ? (priceEl.textContent || '').trim() : '';
                    if (!price && tr.querySelector('span[id^="dprice"]')) {
                        const v = tr.querySelector('span[id^="dprice"][id$="v]"]');
                        if (v) price = (v.textContent || '').trim();
                    }
                    const img = tr.querySelector('img[id^="inlineimg_thumb"], img.listing-inline-image-thumb');
                    let imageUrl = '';
                    if (img && img.src) {
                        imageUrl = img.src.startsWith('http') ? img.src : (baseUrl + (img.src.startsWith('/') ? '' : '/') + img.src);
                    }
                    const manufacturer = mfr ? mfr.textContent?.trim() : '';
                    const part_number = pn ? pn.textContent?.trim() : '';
                    const description = desc ? desc.textContent?.trim() : '';
                    const info_url = infoLink && infoLink.href ? infoLink.href : '';
                    if (!(part_number || manufacturer || info_url)) continue;
                    const priceLower = (price || '').toLowerCase();
                    const isOutOfStock = priceLower.includes('out of stock');
                    const isChooseAtLeft = priceLower.startsWith('choose');
                    if (isOutOfStock) {
                        out.push({
                            manufacturer,
                            part_number,
                            description,
                            price,
                            info_url,
                            market_flags: marketFlags,
                            notes,
                            image_url: imageUrl,
                            sort_group: sortGroup,
                            alternate_oe_numbers: alternateOe
                        });
                    } else if (isChooseAtLeft) {
                        const optionPrices = [];
                        const spans = tbody.querySelectorAll('span.di.vmiddle');
                        for (const span of spans) {
                            const b = span.querySelector('b');
                            if (b) {
                                const p = (b.textContent || '').trim();
                                if (p && /[0-9$€£.,()]/.test(p)) optionPrices.push(p);
                            }
                        }
                        if (optionPrices.length > 0) {
                            for (const p of optionPrices) {
                                out.push({
                                    manufacturer,
                                    part_number,
                                    description,
                                    price: p,
                                    info_url,
                                    market_flags: marketFlags,
                                    notes,
                                    image_url: imageUrl,
                                    sort_group: sortGroup,
                                    alternate_oe_numbers: alternateOe
                                });
                            }
                        } else {
                            out.push({
                                manufacturer,
                                part_number,
                                description,
                                price: null,
                                info_url,
                                market_flags: marketFlags,
                                notes,
                                image_url: imageUrl,
                                sort_group: sortGroup,
                                alternate_oe_numbers: alternateOe
                            });
                        }
                    } else {
                        out.push({
                            manufacturer,
                            part_number,
                            description,
                            price,
                            info_url,
                            market_flags: marketFlags,
                            notes,
                            image_url: imageUrl,
                            sort_group: sortGroup,
                            alternate_oe_numbers: alternateOe
                        });
                    }
                }
            }
            return out;
        }""",
        base_url,
    )
    result = []
    for r in rows:
        manufacturer = (r.get("manufacturer") or "").strip() or None
        part_number = (r.get("part_number") or "").strip() or None
        description = (r.get("description") or "").strip() or None
        raw_price = (r.get("price") or "").strip()
        price = _normalize_price(raw_price) if raw_price else None
        info_url = (r.get("info_url") or "").strip() or None
        market_flags = (r.get("market_flags") or "").strip() or None
        notes = (r.get("notes") or "").strip() or None
        image_url = (r.get("image_url") or "").strip() or None
        sort_group = (r.get("sort_group") or "").strip() or None
        alternate_oe = (r.get("alternate_oe_numbers") or "").strip() or None
        if part_number or manufacturer or info_url:
            result.append({
                "year": year,
                "make": make,
                "model": model,
                "engine": engine or None,
                "category": category,
                "part_type": part_type_name or None,
                "manufacturer": manufacturer,
                "part_number": part_number,
                "price": price,
                "image_url": image_url,
            })
    return result


async def _resolve_choose_price(page: Page, part_number: str | None, manufacturer: str | None) -> str | None:
    """
    For a row that shows 'Choose Type at Left' (or any price starting with 'Choose'),
    find that row, click the first option (select or link), wait for price to update, return new price as-is.
    Returns None if row not found or price could not be resolved.
    """
    if not (part_number or manufacturer):
        return None
    search = (part_number or "") or (manufacturer or "")
    if not search:
        return None
    try:
        tr = page.locator("tr").filter(has_text=search).first
        await tr.wait_for(state="visible", timeout=3000)
    except Exception:
        return None
    try:
        select = tr.locator("select").first
        if await select.count() > 0:
            await select.select_option(index=0)
        else:
            link = tr.locator("a.ra-btn, a[href*='javascript'], .seeopt a, a.navlabellink").first
            if await link.count() > 0:
                await link.click()
        await asyncio.sleep(0.6)
        price_el = tr.locator(".listing-price span, .seeopt-price-text, [id^='dprice'] span, .listing-amount-bold span").first
        if await price_el.count() > 0:
            raw = await price_el.text_content()
            price = (raw or "").strip()
            if price and not price.lower().startswith("choose"):
                return _normalize_price(price) or price
    except Exception:
        pass
    return None


async def _resolve_choose_prices_on_listing(page: Page, products: list[dict]) -> None:
    """Resolve any product whose price starts with 'choose' by clicking first option in that row. Modifies products in place."""
    for p in products:
        price = p.get("price") or ""
        if not str(price).strip().lower().startswith("choose"):
            continue
        resolved = await _resolve_choose_price(
            page,
            p.get("part_number"),
            p.get("manufacturer"),
        )
        if resolved:
            p["price"] = resolved


async def _extract_detail_from_info_page(page: Page) -> dict:
    """
    Extract rich product details from the RockAuto info (moreinfo.php) page.
    Returns a dictionary suitable for JSON serialization.
    """
    return await page.evaluate(
        """() => {
            const pick = (selectors) => {
                for (const sel of selectors) {
                    const el = document.querySelector(sel);
                    if (el && el.textContent) {
                        const txt = el.textContent.trim();
                        if (txt) return txt;
                    }
                }
                return '';
            };
            const title = pick([
                '.detail-card-title',
                '.detail-title',
                '.ra-title',
                '.page-title',
                '.product-title',
                'h1',
                '.content h1',
                '.ra-page-title'
            ]);
            const subtitle = pick([
                '.detail-card-subtitle',
                '.detail-subtitle',
                '.product-subtitle',
                '.detail-heading',
                'h2'
            ]);
            const descriptionSections = Array.from(document.querySelectorAll(
                '.detail-description, .detail_text, .description, #description, .product-description'
            ));
            const description = descriptionSections
                .map(el => el.textContent?.trim() || '')
                .filter(Boolean)
                .join('\\n')
                .trim();
            const specs = [];
            const seen = new Set();
            const collectRows = (table) => {
                Array.from(table.querySelectorAll('tr')).forEach(tr => {
                    const cells = Array.from(tr.querySelectorAll('th, td'));
                    if (cells.length >= 2) {
                        const key = cells[0].innerText.trim();
                        const value = cells.slice(1).map(c => c.innerText.trim()).filter(Boolean).join(' | ');
                        if (key && value) {
                            const hash = key + '::' + value;
                            if (!seen.has(hash)) {
                                specs.push({ label: key, value });
                                seen.add(hash);
                            }
                        }
                    }
                });
            };
            const specTables = Array.from(document.querySelectorAll(
                '.detail-card table, table[class*="spec"], table.specs, table.detail-specs'
            ));
            if (specTables.length === 0) {
                specTables.push(...document.querySelectorAll('table'));
            }
            specTables.forEach(collectRows);
            const bullets = Array.from(document.querySelectorAll('.detail-card ul li, .description ul li'))
                .map(li => li.textContent?.trim() || '')
                .filter(Boolean);
            const images = Array.from(document.querySelectorAll('img'))
                .map(img => img.getAttribute('src') || '')
                .filter(src => src && /\\/info\\//.test(src))
                .map(src => {
                    try {
                        return new URL(src, window.location.href).href;
                    } catch (e) {
                        return src;
                    }
                });
            return {
                title,
                subtitle,
                description,
                specs,
                bullets,
                images,
                url: window.location.href
            };
        }"""
    )


async def _scrape_vehicle_categories(
    page: Page,
    make_slug: str,
    make_name: str,
    year: str,
    model_slug: str,
    model_name: str,
    engine_slug: str,
    engine_name: str,
    add_page_fn,
    db_conn,
) -> tuple[int, bool]:
    """
    Full tree traversal: Vehicle -> Engines -> Categories -> Part Types -> Parts.
    Parts only appear on part-type (leaf) pages (8-segment URLs).
    Returns (parts_count, soft_blocked).
    """
    logger.info("Working on: %s %s %s", year, make_name, model_name)
    vehicle_path = f"{make_slug},{year},{model_slug}"
    vehicle_url = _make_catalog_url(make_slug, year, model_slug, None)

    engines_to_scrape: list[tuple[str, str]] = [(engine_slug, engine_name)] if engine_slug else []
    checkpoint_state = get_checkpoint(db_conn)
    running_total = checkpoint_state.get("total_parts_scraped") or 0

    if not engines_to_scrape:
        url = vehicle_url
        logger.info("  Loading: vehicle page %s", vehicle_path)
        for attempt in range(MAX_RETRIES):
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
                add_page_fn()
            except Exception as e:
                logger.warning("Load %s attempt %s: %s", url, attempt + 1, e)
                await asyncio.sleep(RETRY_DELAY_SEC)
                continue
            break
        else:
            return 0, True
        await _wait_for_catalog_ready(page)
        if await _is_soft_blocked(page):
            logger.warning("Soft block detected on %s", url)
            return 0, True
        # Vehicle page shows engine child trees; products appear only after selecting an engine
        engines_to_scrape = await _get_engines_for_vehicle(page, make_slug, year, model_slug)
        valid = [e for e in engines_to_scrape if e[0]]
        if not valid:
            engines_to_scrape = [("", "")]
            logger.warning("No engine links found for %s %s %s (page has child trees but none matched)", year, make_slug, model_slug)
        else:
            engines_to_scrape = valid
            logger.info("  Found %d engines: %s", len(engines_to_scrape), [e[1] for e in engines_to_scrape])

    total_urls = 0
    for eng_slug, eng_name in engines_to_scrape:
        if _shutdown:
            break
        eng_path = f"{vehicle_path},{eng_slug}" if eng_slug else vehicle_path
        eng_url = _make_catalog_url(make_slug, year, model_slug, eng_slug if eng_slug else None)
        logger.info("  Engine: %s", eng_name or "(all)")
        url = eng_url
        logger.info("  Loading: engine page %s", eng_path[:60] + "..." if len(eng_path) > 60 else eng_path)
        for attempt in range(MAX_RETRIES):
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
                add_page_fn()
            except Exception as e:
                logger.warning("Load %s attempt %s: %s", url, attempt + 1, e)
                await asyncio.sleep(RETRY_DELAY_SEC)
                continue
            break
        else:
            continue
        await _wait_for_catalog_ready(page)
        if await _is_soft_blocked(page):
            logger.warning("Soft block detected on %s", url)
            return total_urls, True

        categories = await _get_category_links_for_vehicle(
            page, make_slug, year, model_slug, eng_slug or ""
        )
        if categories:
            logger.info("  Found %d categories: %s", len(categories), [c[1] for c in categories[:8]] + (["..."] if len(categories) > 8 else []))
        for cat_path, cat_name in categories:
            if _shutdown:
                break
            logger.info("    Category: %s", cat_name)
            cat_url = f"{CATALOG_URL}{cat_path}"
            logger.info("    Loading: category %s", cat_name)
            try:
                await page.goto(cat_url, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT)
                add_page_fn()
            except Exception as e:
                logger.debug("Category %s: %s", cat_url, e)
                continue
            await _wait_for_catalog_ready(page)
            if await _is_soft_blocked(page):
                logger.warning("Soft block on category %s", cat_url)
                return total_urls, True

            part_types = await _get_part_type_links_from_category_page(page, cat_path)
            if part_types:
                logger.info("      Found %d part types: %s", len(part_types), [p[1] for p in part_types[:6]] + (["..."] if len(part_types) > 6 else []))
            for pt_path, pt_name in part_types:
                if _shutdown:
                    break
                pt_url = f"{CATALOG_URL}{pt_path}"
                logger.info("      Part type: %s", pt_name)
                logger.info("      Loading: listing %s > %s", cat_name, pt_name)
                try:
                    await page.goto(pt_url, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT)
                    add_page_fn()
                except Exception as e:
                    logger.debug("Part type %s: %s", pt_url, e)
                    continue
                await _wait_for_catalog_ready(page)
                if await _is_soft_blocked(page):
                    logger.warning("Soft block on part type %s", pt_url)
                    return total_urls, True

                products = await _collect_products_from_listing(
                    page, year, make_name, model_name, eng_name or "", cat_name, pt_name
                )
                if products:
                    logger.info("      %s > %s: %d products", cat_name, pt_name, len(products))
                    insert_product_urls(db_conn, products)
                    total_urls += len(products)
                    logger.info("      [tree] Saved %d product URLs under %s > %s", len(products), cat_name, pt_name)
                    running_total += len(products)
                    save_checkpoint(
                        db_conn,
                        last_make_slug=make_slug,
                        last_year=year,
                        last_model_slug=model_slug,
                        last_engine_slug=eng_slug or None,
                        total_parts_scraped=running_total,
                    )
                    db_conn.commit()

    return total_urls, False


async def _scrape_vehicle_to_rockauto(
    page: Page,
    make_slug: str,
    make_name: str,
    year: str,
    model_slug: str,
    model_name: str,
    add_page_fn,
    rockauto_conn,
) -> tuple[int, bool]:
    """
    Same traversal as _scrape_vehicle_categories but saves to rockauto.db (only id, year, make, model,
    engine, category, part_type, manufacturer, part_number, price, scraped_at). Returns (parts_count, soft_blocked).
    """
    logger.info("Scraping to rockauto: %s %s %s", year, make_name, model_name)
    deleted = delete_rockauto_by_vehicle(rockauto_conn, year, make_name, model_name)
    if deleted:
        logger.info("  Deleted %d existing rows for this vehicle (replacing with fresh scrape)", deleted)
    rockauto_conn.commit()

    vehicle_path = f"{make_slug},{year},{model_slug}"
    vehicle_url = _make_catalog_url(make_slug, year, model_slug, None)

    engines_to_scrape: list[tuple[str, str]] = []
    url = vehicle_url
    logger.info("  Loading: vehicle page %s", vehicle_path)
    for attempt in range(MAX_RETRIES):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
            add_page_fn()
        except Exception as e:
            logger.warning("Load %s attempt %s: %s", url, attempt + 1, e)
            await asyncio.sleep(RETRY_DELAY_SEC)
            continue
        break
    else:
        return 0, True
    await _wait_for_catalog_ready(page)
    if await _is_soft_blocked(page):
        logger.warning("Soft block detected on %s", url)
        return 0, True
    engines_to_scrape = await _get_engines_for_vehicle(page, make_slug, year, model_slug)
    valid = [e for e in engines_to_scrape if e[0]]
    if not valid:
        engines_to_scrape = [("", "")]
        logger.warning("No engine links found for %s %s %s", year, make_slug, model_slug)
    else:
        engines_to_scrape = valid
        logger.info("  Found %d engines: %s", len(engines_to_scrape), [e[1] for e in engines_to_scrape])

    total_parts = 0
    for eng_slug, eng_name in engines_to_scrape:
        if _shutdown:
            break
        eng_url = _make_catalog_url(make_slug, year, model_slug, eng_slug if eng_slug else None)
        logger.info("  Engine: %s", eng_name or "(all)")
        for attempt in range(MAX_RETRIES):
            try:
                await page.goto(eng_url, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
                add_page_fn()
            except Exception as e:
                logger.warning("Load %s attempt %s: %s", eng_url, attempt + 1, e)
                await asyncio.sleep(RETRY_DELAY_SEC)
                continue
            break
        else:
            continue
        await _wait_for_catalog_ready(page)
        if await _is_soft_blocked(page):
            logger.warning("Soft block detected on %s", eng_url)
            return total_parts, True

        categories = await _get_category_links_for_vehicle(page, make_slug, year, model_slug, eng_slug or "")
        if categories:
            logger.info("  Found %d categories", len(categories))
        for cat_path, cat_name in categories:
            if _shutdown:
                break
            cat_url = f"{CATALOG_URL}{cat_path}"
            try:
                await page.goto(cat_url, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT)
                add_page_fn()
            except Exception as e:
                logger.debug("Category %s: %s", cat_url, e)
                continue
            await _wait_for_catalog_ready(page)
            if await _is_soft_blocked(page):
                return total_parts, True

            part_types = await _get_part_type_links_from_category_page(page, cat_path)
            for pt_path, pt_name in part_types:
                if _shutdown:
                    break
                pt_url = f"{CATALOG_URL}{pt_path}"
                try:
                    await page.goto(pt_url, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT)
                    add_page_fn()
                except Exception as e:
                    logger.debug("Part type %s: %s", pt_url, e)
                    continue
                await _wait_for_catalog_ready(page)
                if await _is_soft_blocked(page):
                    return total_parts, True

                products = await _collect_products_from_listing(
                    page, year, make_name, model_name, eng_name or "", cat_name, pt_name
                )
                if products:
                    await _resolve_choose_prices_on_listing(page, products)
                    insert_rockauto(rockauto_conn, products)
                    rockauto_conn.commit()
                    total_parts += len(products)
                    logger.info("      %s > %s: %d products -> rockauto.db", cat_name, pt_name, len(products))

    return total_parts, False


# --------------- Load vehicles from CSV or discover ---------------
def _load_vehicles_csv(resume_after: dict | None = None) -> list[dict]:
    """Load vehicles from CSV; if resume_after is set, skip vehicles <= checkpoint (resume support)."""
    if not VEHICLES_CSV.exists():
        return []
    last_make = (resume_after or {}).get("last_make_slug") or ""
    last_year = (resume_after or {}).get("last_year") or ""
    last_model = (resume_after or {}).get("last_model_slug") or ""
    out = []
    with open(VEHICLES_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            year = (row.get("year") or "").strip()
            make = (row.get("make") or "").strip()
            model = (row.get("model") or "").strip()
            if year and make and model:
                make_slug = make.lower().replace(" ", "+").replace("-", "-")
                model_slug = model.lower().replace(" ", "+").replace("-", "-")
                if (make_slug, year, model_slug) <= (last_make, last_year, last_model):
                    continue
                out.append({
                    "year": year,
                    "make": make,
                    "model": model,
                    "make_slug": make_slug,
                    "model_slug": model_slug,
                    "engine_slug": (row.get("engine_slug") or "").strip() or None,
                    "engine_name": (row.get("engine_name") or "").strip() or None,
                })
    return out


async def _discover_vehicles(page: Page, resume_after: dict, progress_bar: bool = True) -> list[dict]:
    """Build full vehicle list (make, year, model) from catalog; respect resume_after to skip already-done. No catalog tree persisted."""
    makes = await _get_makes_from_catalog(page)
    tasks = []
    last_make = resume_after.get("last_make_slug") or ""
    last_year = resume_after.get("last_year") or ""
    last_model = resume_after.get("last_model_slug") or ""
    make_iter = tqdm(makes, desc="Discovering", unit="make", total=len(makes), ncols=100) if progress_bar else makes
    for make_slug, make_name in make_iter:
        try:
            years = await _get_years_for_make(page, make_slug)
        except Exception as e:
            logger.warning("Skipping make %s: %s", make_slug, e)
            if progress_bar:
                make_iter.set_postfix_str(f"{make_name} (skipped)")
            continue
        for year in years:
            try:
                models = await _get_models_for_make_year(page, make_slug, year)
            except Exception as e:
                logger.warning("Skipping %s %s: %s", make_slug, year, e)
                continue
            for model_slug, model_name in models:
                # Only add if (make_slug, year, model_slug) is strictly after (last_make, last_year, last_model)
                if (make_slug, year, model_slug) <= (last_make, last_year, last_model):
                    continue
                tasks.append({
                    "make_slug": make_slug,
                    "make": make_name,
                    "year": year,
                    "model_slug": model_slug,
                    "model": model_name,
                    "engine_slug": None,
                    "engine_name": None,
                })
        if progress_bar:
            make_iter.set_postfix_str(f"{make_name} ({len(tasks)} vehicles)")
    return tasks


# --------------- Worker: process one vehicle; own context with recycling ---------------
async def _worker(
    browser: Browser,
    queue: asyncio.Queue,
    on_route,
    add_page_fn,
    db_conn_factory,
):
    import time
    wrapper: ContextWrapper | None = None

    async def get_or_create_context():
        nonlocal wrapper
        if wrapper is not None and not wrapper.should_recycle():
            return wrapper.context
        if wrapper is not None:
            try:
                await wrapper.context.close()
            except Exception:
                pass
            wrapper = None
        ctx = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="en-US",
            timezone_id="America/New_York",
        )
        await ctx.route("**/*", on_route)
        wrapper = ContextWrapper(ctx, time.time(), CONTEXT_MAX_PAGES, CONTEXT_MAX_MINUTES)
        return ctx

    while not _shutdown:
        try:
            task = await asyncio.wait_for(queue.get(), timeout=2.0)
        except asyncio.TimeoutError:
            continue
        if task is None:
            break
        ctx = await get_or_create_context()
        page = await ctx.new_page()
        if STEALTH_AVAILABLE and stealth_async:
            await stealth_async(page, StealthConfig())
        db_conn = db_conn_factory()
        try:
            count, soft_blocked = await _scrape_vehicle_categories(
                page,
                task["make_slug"],
                task["make"],
                task["year"],
                task["model_slug"],
                task["model"],
                task.get("engine_slug") or "",
                task.get("engine_name") or "",
                add_page_fn,
                db_conn,
            )
            if wrapper:
                wrapper.pages_done += 1
            if soft_blocked and wrapper:
                try:
                    await wrapper.context.close()
                except Exception:
                    pass
                wrapper = None
            logger.info("Vehicle %s %s %s: %d parts", task["year"], task["make"], task["model"], count)
        except Exception as e:
            logger.exception("Vehicle %s: %s", task, e)
        finally:
            await page.close()
            db_conn.close()
        queue.task_done()

    if wrapper:
        try:
            await wrapper.context.close()
        except Exception:
            pass


async def _process_detail_queue(keep_browser_open: bool | None = None):
    """
    Worker loop: take Info URLs from product_urls table (catalog_product.db), fetch detail page,
    insert into rockauto table (rockauto.db) and mark URL as done.
    """
    if keep_browser_open is None:
        keep_browser_open = KEEP_BROWSER_OPEN
    init_db()
    init_catalog_product_db()
    on_route, add_page_fn, bw_data = _create_bandwidth_tracker()
    proxy_url = get_proxy_url()
    logger.info("Launching browser (details stage)...")
    async with async_playwright() as p:
        launch_options = {
            "headless": HEADLESS,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-accelerated-2d-canvas",
                "--disable-gpu",
            ],
        }
        if proxy_url and "@" in proxy_url:
            try:
                after_http = proxy_url.split("://", 1)[1]
                user_pass, server = after_http.rsplit("@", 1)
                u, pw = user_pass.split(":", 1) if ":" in user_pass else (user_pass, "")
                launch_options["proxy"] = {
                    "server": f"http://{server}",
                    "username": u,
                    "password": pw,
                }
            except Exception as e:
                logger.warning("Proxy URL parse failed: %s; running without proxy.", e)
        browser = await p.chromium.launch(**launch_options)
        logger.info("Browser launched (details stage).")
        ctx = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="en-US",
            timezone_id="America/New_York",
        )
        await ctx.route("**/*", on_route)
        cp_conn = get_catalog_product_connection()
        rockauto_conn = get_connection()
        try:
            while not _shutdown:
                pending_rows = fetch_product_urls(cp_conn, status="pending", limit=10)
                if not pending_rows:
                    await asyncio.sleep(5)
                    continue
                ids = [row["id"] for row in pending_rows]
                mark_product_urls_in_progress(cp_conn, ids)
                cp_conn.commit()
                logger.info("Details stage: fetching %d product pages...", len(pending_rows))
                for row in pending_rows:
                    if _shutdown:
                        break
                    info_url = row["info_url"]
                    page = await ctx.new_page()
                    if STEALTH_AVAILABLE and stealth_async:
                        await stealth_async(page, StealthConfig())
                    try:
                        await page.goto(info_url, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT)
                        add_page_fn()
                        insert_rockauto(rockauto_conn, [{
                            "year": row.get("year"),
                            "make": row.get("make"),
                            "model": row.get("model"),
                            "engine": row.get("engine"),
                            "category": row.get("category"),
                            "part_type": row.get("part_type"),
                            "manufacturer": row.get("manufacturer"),
                            "part_number": row.get("part_number"),
                            "price": _normalize_price(row.get("listing_price") or "") or row.get("listing_price"),
                            "scraped_at": datetime.now().isoformat(),
                        }])
                        rockauto_conn.commit()
                        mark_product_url_done(cp_conn, row["id"])
                        cp_conn.commit()
                        logger.info("  Detail: %s %s %s — %s", row.get("year") or "", row.get("make") or "", row.get("model") or "", (row.get("part_number") or "")[:40] or "info")
                    except Exception as e:
                        rockauto_conn.rollback()
                        cp_conn.rollback()
                        mark_product_url_error(cp_conn, row["id"], str(e))
                        cp_conn.commit()
                        logger.warning("Detail scrape failed for %s: %s", info_url, e)
                    finally:
                        await page.close()
        finally:
            cp_conn.close()
            rockauto_conn.close()
            await ctx.close()
            await _wait_before_close(keep_browser_open)
            await browser.close()


def _build_launch_options():
    """Shared Playwright launch options (proxy, headless, args)."""
    proxy_url = get_proxy_url()
    opts = {
        "headless": HEADLESS,
        "args": [
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shim-usage",
            "--disable-accelerated-2d-canvas",
            "--disable-gpu",
        ],
    }
    if proxy_url and "@" in proxy_url:
        try:
            after_http = proxy_url.split("://", 1)[1]
            user_pass, server = after_http.rsplit("@", 1)
            u, pw = user_pass.split(":", 1) if ":" in user_pass else (user_pass, "")
            opts["proxy"] = {"server": f"http://{server}", "username": u, "password": pw}
        except Exception as e:
            logger.warning("Proxy URL parse failed: %s; running without proxy.", e)
    return opts


async def run_export_make_year_model(keep_browser_open: bool | None = None) -> int:
    """
    Step 1 only: discover all make/year/model from the catalog and export to make_year_model.csv.
    Returns number of rows written to CSV. Use this script alone to refresh the CSV.
    """
    init_catalog_product_db()
    conn = get_catalog_product_connection()
    checkpoint = get_checkpoint(conn)
    conn.close()

    on_route, add_page_fn, bw_data = _create_bandwidth_tracker()
    launch_options = _build_launch_options()

    logger.info("Step 1: Export make/year/model CSV – launching browser...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(**launch_options)
        ctx = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="en-US",
            timezone_id="America/New_York",
        )
        await ctx.route("**/*", on_route)
        page = await ctx.new_page()
        if STEALTH_AVAILABLE and stealth_async:
            await stealth_async(page, StealthConfig())
        vehicles = await _discover_vehicles(page, checkpoint)
        n_csv = _write_make_year_model_csv(vehicles)
        if n_csv:
            logger.info("Exported %d make/year/model pairs to %s", n_csv, MAKE_YEAR_MODEL_CSV)
        await page.close()
        await ctx.close()
        await browser.close()
        await _wait_before_close(keep_browser_open if keep_browser_open is not None else KEEP_BROWSER_OPEN)
    logger.info("Step 1 finished. Bandwidth: %.1f KB for %d pages", bw_data["bytes"] / 1024, bw_data["pages"])
    return n_csv or 0


async def run_scrape_from_csv(keep_browser_open: bool | None = None) -> int:
    """
    Step 2 only: load make_year_model.csv and scrape each vehicle's products into rockauto.db.
    Returns total products saved. Run after run_export_make_year_model (or use an existing CSV).
    """
    init_db()
    rows_csv = _load_make_year_model_csv()
    if not rows_csv:
        logger.warning("No rows in %s. Run export_make_year_model.py first to generate it.", MAKE_YEAR_MODEL_CSV)
        return 0

    on_route, add_page_fn, bw_data = _create_bandwidth_tracker()
    launch_options = _build_launch_options()

    logger.info("Step 2: Scrape from CSV – loaded %d rows from %s. Launching browser...", len(rows_csv), MAKE_YEAR_MODEL_CSV)
    async with async_playwright() as p:
        browser = await p.chromium.launch(**launch_options)
        ctx = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="en-US",
            timezone_id="America/New_York",
        )
        await ctx.route("**/*", on_route)
        page = await ctx.new_page()
        if STEALTH_AVAILABLE and stealth_async:
            await stealth_async(page, StealthConfig())
        rockauto_conn = get_connection()
        try:
            total_products = 0
            total_vehicles = len(rows_csv)
            pbar = tqdm(rows_csv, desc="Vehicles", unit="vehicle", total=total_vehicles, ncols=100)
            for row in pbar:
                if _shutdown:
                    break
                url = (row.get("url") or "").strip()
                if not url or "/en/catalog/" not in url:
                    pbar.set_postfix_str("skipped")
                    continue
                path = url.split("/en/catalog/")[-1].strip("/").split("?")[0]
                parts = [p.strip() for p in path.split(",") if p.strip()]
                if len(parts) < 3:
                    pbar.set_postfix_str("skipped")
                    continue
                make_slug, year, model_slug = parts[0], parts[1], parts[2]
                make_name = row.get("make") or _slug_to_label(make_slug)
                model_name = row.get("model") or _slug_to_label(model_slug)
                pbar.set_postfix_str(f"{year} {make_name} {model_name}")
                n, soft_blocked = await _scrape_vehicle_to_rockauto(
                    page, make_slug, make_name, year, model_slug, model_name, add_page_fn, rockauto_conn
                )
                total_products += n
                pbar.set_postfix_str(f"{year} {make_name} {model_name} ({n} parts)")
                if soft_blocked:
                    logger.warning("Stopping scrape due to soft block.")
                    break
            if total_products:
                logger.info("Saved %d products to rockauto.db", total_products)
        finally:
            rockauto_conn.close()
        await page.close()
        await ctx.close()
        await browser.close()
        await _wait_before_close(keep_browser_open if keep_browser_open is not None else KEEP_BROWSER_OPEN)
    logger.info("Step 2 finished. Bandwidth: %.1f KB for %d pages", bw_data["bytes"] / 1024, bw_data["pages"])
    return total_products


async def _run_scraper(keep_browser_open: bool | None = None):
    """Discover full catalog (all makes/years/models) and scrape all products. No vehicles.csv."""
    init_db()
    init_catalog_product_db()
    conn = get_catalog_product_connection()
    checkpoint = get_checkpoint(conn)
    conn.close()

    on_route, add_page_fn, bw_data = _create_bandwidth_tracker()
    launch_options = _build_launch_options()

    logger.info("Launching browser (discover stage – full catalog, all products)...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(**launch_options)
        logger.info("Browser launched (discover stage).")
        ctx = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="en-US",
            timezone_id="America/New_York",
        )
        await ctx.route("**/*", on_route)
        page = await ctx.new_page()
        if STEALTH_AVAILABLE and stealth_async:
            await stealth_async(page, StealthConfig())
        vehicles = await _discover_vehicles(page, checkpoint)
        # Phase 1: Export all make/year/model pairs to CSV (no catalog tree built)
        n_csv = _write_make_year_model_csv(vehicles)
        if n_csv:
            logger.info("Exported %d make/year/model pairs to %s", n_csv, MAKE_YEAR_MODEL_CSV)
        await page.close()
        await ctx.close()
        await browser.close()
        logger.info("Phase 1 done. Browser closed.")

        # Phase 2: Load make/year/model CSV and scrape each vehicle (engines -> categories -> part types -> products) into rockauto.db
        rows_csv = _load_make_year_model_csv()
        if not rows_csv:
            logger.warning("No rows in %s; skipping product scrape. Run Phase 1 first to generate the CSV.", MAKE_YEAR_MODEL_CSV)
        else:
            logger.info("Loaded %d make/year/model rows from %s. Starting scrape (one browser, one vehicle at a time)...", len(rows_csv), MAKE_YEAR_MODEL_CSV)
            browser2 = await p.chromium.launch(**launch_options)
            ctx2 = await browser2.new_context(
                viewport={"width": 1280, "height": 720},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                locale="en-US",
                timezone_id="America/New_York",
            )
            await ctx2.route("**/*", on_route)
            page2 = await ctx2.new_page()
            if STEALTH_AVAILABLE and stealth_async:
                await stealth_async(page2, StealthConfig())
            rockauto_conn = get_connection()
            try:
                total_products = 0
                total_vehicles = len(rows_csv)
                pbar = tqdm(rows_csv, desc="Vehicles", unit="vehicle", total=total_vehicles, ncols=100)
                for row in pbar:
                    if _shutdown:
                        break
                    url = (row.get("url") or "").strip()
                    if not url or "/en/catalog/" not in url:
                        pbar.set_postfix_str("skipped")
                        continue
                    path = url.split("/en/catalog/")[-1].strip("/").split("?")[0]
                    parts = [p.strip() for p in path.split(",") if p.strip()]
                    if len(parts) < 3:
                        pbar.set_postfix_str("skipped")
                        continue
                    make_slug, year, model_slug = parts[0], parts[1], parts[2]
                    make_name = row.get("make") or _slug_to_label(make_slug)
                    model_name = row.get("model") or _slug_to_label(model_slug)
                    pbar.set_postfix_str(f"{year} {make_name} {model_name}")
                    n, soft_blocked = await _scrape_vehicle_to_rockauto(
                        page2, make_slug, make_name, year, model_slug, model_name, add_page_fn, rockauto_conn
                    )
                    total_products += n
                    pbar.set_postfix_str(f"{year} {make_name} {model_name} ({n} parts)")
                    if soft_blocked:
                        logger.warning("Stopping scrape due to soft block.")
                        break
                if total_products:
                    logger.info("Saved %d products to rockauto.db (id, year, make, model, engine, category, part_type, manufacturer, part_number, price, scraped_at)", total_products)
            finally:
                rockauto_conn.close()
            await page2.close()
            await ctx2.close()
            await browser2.close()
            logger.info("Product scraping finished. Browser closed.")

        await _wait_before_close(keep_browser_open if keep_browser_open is not None else KEEP_BROWSER_OPEN)

    logger.info("Scrape finished. Total bandwidth: %.1f KB for %d pages", bw_data["bytes"] / 1024, bw_data["pages"])


def main():
    ap = argparse.ArgumentParser(description="RockAuto scraper pipeline")
    ap.add_argument(
        "stage",
        choices=["discover", "details"],
        nargs="?",
        default="discover",
        help="Run discovery (catalog traversal, collect URLs) or details (fetch product info pages)",
    )
    ap.add_argument("--keep-browser-open", action="store_true", help="After run, wait for Enter before closing browser (default when browser is visible)")
    ap.add_argument("--no-keep-browser-open", action="store_true", dest="no_keep_browser_open", help="Close browser immediately when done (no Enter)")
    args = ap.parse_args()

    keep_browser_open = KEEP_BROWSER_OPEN
    if args.keep_browser_open:
        keep_browser_open = True
    if getattr(args, "no_keep_browser_open", False):
        keep_browser_open = False

    signal.signal(signal.SIGTERM, _set_shutdown)
    signal.signal(signal.SIGINT, _set_shutdown)
    if args.stage == "details":
        asyncio.run(_process_detail_queue(keep_browser_open=keep_browser_open))
    else:
        asyncio.run(_run_scraper(keep_browser_open=keep_browser_open))


if __name__ == "__main__":
    main()
