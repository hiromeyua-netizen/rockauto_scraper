"""
RockAuto SQLite schema (WAL mode).
rockauto.db: single rockauto table (id, year, make, model, engine, category, part_type, manufacturer, part_number, price, scraped_at).
catalog_product.db: catalog_product, product_urls, checkpoint.
"""
import sqlite3
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "rockauto.db"

try:
    from rockauto.config import CATALOG_PRODUCT_DB_PATH
except ImportError:
    CATALOG_PRODUCT_DB_PATH = BASE_DIR / "catalog_product.db"

# Root parent id for top-level catalogs (no parent)
CATALOG_PRODUCT_ROOT_PARENT_ID = "0" * 24


def get_catalog_product_connection() -> sqlite3.Connection:
    """Connection to catalog_product.db (catalog + product URLs)."""
    conn = sqlite3.connect(CATALOG_PRODUCT_DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.row_factory = sqlite3.Row
    return conn


def init_catalog_product_db() -> None:
    """Create catalog_product table: id, parentId, no, url, isCatalog (boolean), scrapeStatus (boolean, default False), createdAt."""
    with get_catalog_product_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS catalog_product (
                id TEXT PRIMARY KEY,
                parentId TEXT NOT NULL,
                no TEXT NOT NULL,
                url TEXT NOT NULL,
                isCatalog INTEGER NOT NULL,
                scrapeStatus INTEGER NOT NULL DEFAULT 0,
                createdAt TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_catalog_product_url ON catalog_product(url)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_catalog_product_parent ON catalog_product(parentId)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_catalog_product_is_catalog ON catalog_product(isCatalog)")
        _migrate_catalog_product_no_to_text(conn)
        _migrate_catalog_product_booleans(conn)
        _init_catalog_product_extras(conn)


def _migrate_catalog_product_no_to_text(conn: sqlite3.Connection) -> None:
    """Migrate catalog_product.no from INTEGER to TEXT (for 1-1, 1-2 format)."""
    try:
        for row in conn.execute("PRAGMA table_info(catalog_product)"):
            if row[1] == "no" and (row[2] or "").upper() == "INTEGER":
                conn.execute("ALTER TABLE catalog_product RENAME TO catalog_product_old")
                conn.execute("""
                    CREATE TABLE catalog_product (
                        id TEXT PRIMARY KEY,
                        parentId TEXT NOT NULL,
                        no TEXT NOT NULL,
                        url TEXT NOT NULL,
                        isCatalog INTEGER NOT NULL,
                        scrapeStatus INTEGER NOT NULL DEFAULT 0,
                        createdAt TEXT DEFAULT (datetime('now'))
                    )
                """)
                conn.execute("""
                    INSERT INTO catalog_product (id, parentId, no, url, isCatalog, scrapeStatus, createdAt)
                    SELECT id, parentId, CAST(no AS TEXT), url,
                        CASE WHEN isCatalog IN ('true','1',1) THEN 1 ELSE 0 END,
                        COALESCE(CASE WHEN scrapeStatus IN ('true','1',1) THEN 1 ELSE 0 END, 0),
                        createdAt
                    FROM catalog_product_old
                """)
                conn.execute("DROP TABLE catalog_product_old")
                conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_catalog_product_url ON catalog_product(url)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_catalog_product_parent ON catalog_product(parentId)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_catalog_product_is_catalog ON catalog_product(isCatalog)")
                break
    except sqlite3.OperationalError:
        pass


def _migrate_catalog_product_booleans(conn: sqlite3.Connection) -> None:
    """Migrate isCatalog/scrapeStatus from TEXT/nullable to INTEGER NOT NULL (boolean: 1=True, 0=False)."""
    try:
        info = {row[1]: (row[2] or "").upper() for row in conn.execute("PRAGMA table_info(catalog_product)")}
        if info.get("isCatalog") == "TEXT" or info.get("scrapeStatus") in ("TEXT", ""):
            conn.execute("ALTER TABLE catalog_product RENAME TO catalog_product_old")
            conn.execute("""
                CREATE TABLE catalog_product (
                    id TEXT PRIMARY KEY,
                    parentId TEXT NOT NULL,
                    no TEXT NOT NULL,
                    url TEXT NOT NULL,
                    isCatalog INTEGER NOT NULL,
                    scrapeStatus INTEGER NOT NULL DEFAULT 0,
                    createdAt TEXT DEFAULT (datetime('now'))
                )
            """)
            conn.execute("""
                INSERT INTO catalog_product (id, parentId, no, url, isCatalog, scrapeStatus, createdAt)
                SELECT id, parentId, no, url,
                    CASE WHEN isCatalog IN ('true','1',1) THEN 1 ELSE 0 END,
                    COALESCE(CASE WHEN scrapeStatus IN ('true','1',1) THEN 1 ELSE 0 END, 0),
                    createdAt
                FROM catalog_product_old
            """)
            conn.execute("DROP TABLE catalog_product_old")
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_catalog_product_url ON catalog_product(url)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_catalog_product_parent ON catalog_product(parentId)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_catalog_product_is_catalog ON catalog_product(isCatalog)")
    except sqlite3.OperationalError:
        pass


def insert_first_level_catalogs(conn: sqlite3.Connection, urls: list[str]) -> int:
    """
    Insert top-level catalog URLs (from first page). id = 24-char zero-padded no, parentId = 0*24, no = 1,2,3..., isCatalog = True, scrapeStatus = False.
    Returns number of rows inserted (skips duplicates by url).
    """
    if not urls:
        return 0
    inserted = 0
    for no, url in enumerate(urls, start=1):
        id_24 = str(no).zfill(24)
        try:
            conn.execute(
                """
                INSERT INTO catalog_product (id, parentId, no, url, isCatalog, scrapeStatus, createdAt)
                VALUES (?, ?, ?, ?, 1, 0, datetime('now'))
                """,
                (id_24, CATALOG_PRODUCT_ROOT_PARENT_ID, str(no), url),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            pass  # url already exists
    return inserted


def fetch_first_level_catalogs(conn: sqlite3.Connection) -> list:
    """Fetch catalog_product rows where parentId = root (first-level catalogs)."""
    return conn.execute(
        "SELECT * FROM catalog_product WHERE parentId = ? ORDER BY no",
        (CATALOG_PRODUCT_ROOT_PARENT_ID,),
    ).fetchall()


def _path_segment_count(url: str) -> int:
    """Return number of comma-separated segments in catalog path (e.g. abarth,1969,1000 -> 3)."""
    url = (url or "").split("?")[0]
    if "/en/catalog/" not in url:
        return 0
    path = url.split("/en/catalog/")[-1].strip("/")
    return path.count(",") + 1 if path else 0


def fetch_catalogs_by_path_segment_count(conn: sqlite3.Connection, segment_count: int) -> list:
    """Fetch catalog_product rows where url path has exactly segment_count comma-separated parts (catalogs only)."""
    rows = conn.execute(
        "SELECT * FROM catalog_product WHERE isCatalog = 1 ORDER BY no",
    ).fetchall()
    return [row for row in rows if _path_segment_count(row["url"] or "") == segment_count]


def fetch_year_level_catalogs(conn: sqlite3.Connection) -> list:
    """Fetch catalog_product rows that are year-level (url path has exactly 2 segments: make,year)."""
    return fetch_catalogs_by_path_segment_count(conn, 2)


def get_catalog_by_url(conn: sqlite3.Connection, url: str) -> sqlite3.Row | None:
    """Get catalog_product row by url. Returns None if not found."""
    return conn.execute("SELECT * FROM catalog_product WHERE url = ?", (url,)).fetchone()


def _next_catalog_id(conn: sqlite3.Connection) -> str:
    """Generate next 24-char unique id for catalog_product (max existing + 1)."""
    row = conn.execute("SELECT id FROM catalog_product ORDER BY CAST(id AS INTEGER) DESC LIMIT 1").fetchone()
    if not row:
        return "1".zfill(24)
    try:
        next_val = int(row[0]) + 1
    except (ValueError, TypeError):
        next_val = int("".join(c for c in str(row[0]) if c.isdigit()) or "0") + 1
    return str(next_val).zfill(24)


def insert_child_catalogs(
    conn: sqlite3.Connection,
    parent_id: str,
    parent_no: str,
    child_urls: list[str],
) -> int:
    """
    Insert child catalogs under a parent. parentId=parent_id, no=parent_no-1, parent_no-2, ...
    Returns number inserted (skips duplicates by url).
    """
    if not child_urls:
        return 0
    inserted = 0
    for i, url in enumerate(child_urls, start=1):
        no_str = f"{parent_no}-{i}"
        id_24 = _next_catalog_id(conn)
        try:
            conn.execute(
                """
                INSERT INTO catalog_product (id, parentId, no, url, isCatalog, scrapeStatus, createdAt)
                VALUES (?, ?, ?, ?, 1, 0, datetime('now'))
                """,
                (id_24, parent_id, no_str, url),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            pass
    return inserted


def insert_child_products(
    conn: sqlite3.Connection,
    parent_id: str,
    parent_no: str,
    product_urls: list[str],
) -> int:
    """
    Insert product URLs as rows in catalog_product. isCatalog=0, scrapeStatus=0, no=parent_no-1, parent_no-2, ...
    Returns number inserted (skips duplicates by url).
    """
    if not product_urls:
        return 0
    inserted = 0
    for i, url in enumerate(product_urls, start=1):
        if not (url or "").strip():
            continue
        no_str = f"{parent_no}-{i}"
        id_24 = _next_catalog_id(conn)
        try:
            conn.execute(
                """
                INSERT INTO catalog_product (id, parentId, no, url, isCatalog, scrapeStatus, createdAt)
                VALUES (?, ?, ?, ?, 0, 0, datetime('now'))
                """,
                (id_24, parent_id, no_str, url.strip()),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            pass
    return inserted


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Create rockauto table: id, year, make, model, engine, category, part_type, manufacturer, part_number, price, image_url, scraped_at."""
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS rockauto (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year TEXT,
                make TEXT,
                model TEXT,
                engine TEXT,
                category TEXT,
                part_type TEXT,
                manufacturer TEXT,
                part_number TEXT,
                price TEXT,
                image_url TEXT,
                scraped_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_rockauto_scraped_at ON rockauto(scraped_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_rockauto_make_model ON rockauto(make, model, year)")
        _add_rockauto_image_url_if_missing(conn)


def _add_rockauto_image_url_if_missing(conn: sqlite3.Connection) -> None:
    """Add image_url column if missing (for existing DBs)."""
    info = {row[1]: row[2] for row in conn.execute("PRAGMA table_info(rockauto)")}
    if "image_url" not in info:
        try:
            conn.execute("ALTER TABLE rockauto ADD COLUMN image_url TEXT")
        except sqlite3.OperationalError:
            pass


def _init_catalog_product_extras(conn: sqlite3.Connection) -> None:
    """Create product_urls and checkpoint tables in catalog_product.db for scraper queue and resume."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS product_urls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            info_url TEXT NOT NULL UNIQUE,
            discovered_at TEXT NOT NULL,
            year TEXT, make TEXT, model TEXT, engine TEXT,
            category TEXT, part_type TEXT, manufacturer TEXT, part_number TEXT, listing_price TEXT,
            status TEXT NOT NULL DEFAULT 'pending', last_error TEXT, detail_scraped_at TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_product_urls_status ON product_urls(status)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS checkpoint (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_make_slug TEXT, last_year TEXT, last_model_slug TEXT,
            last_engine_slug TEXT, last_category_slug TEXT, last_page INTEGER,
            total_pages_scraped INTEGER DEFAULT 0, total_parts_scraped INTEGER DEFAULT 0,
            updated_at TEXT NOT NULL
        )
    """)
    conn.execute("INSERT OR IGNORE INTO checkpoint (id, updated_at) VALUES (1, datetime('now'))")


def save_checkpoint(
    conn: sqlite3.Connection,
    *,
    last_make_slug: str | None = None,
    last_year: str | None = None,
    last_model_slug: str | None = None,
    last_engine_slug: str | None = None,
    last_category_slug: str | None = None,
    last_page: int | None = None,
    total_pages_scraped: int | None = None,
    total_parts_scraped: int | None = None,
) -> None:
    updates = ["updated_at = datetime('now')"]
    params: list = []
    if last_make_slug is not None:
        updates.append("last_make_slug = ?")
        params.append(last_make_slug)
    if last_year is not None:
        updates.append("last_year = ?")
        params.append(last_year)
    if last_model_slug is not None:
        updates.append("last_model_slug = ?")
        params.append(last_model_slug)
    if last_engine_slug is not None:
        updates.append("last_engine_slug = ?")
        params.append(last_engine_slug)
    if last_category_slug is not None:
        updates.append("last_category_slug = ?")
        params.append(last_category_slug)
    if last_page is not None:
        updates.append("last_page = ?")
        params.append(last_page)
    if total_pages_scraped is not None:
        updates.append("total_pages_scraped = ?")
        params.append(total_pages_scraped)
    if total_parts_scraped is not None:
        updates.append("total_parts_scraped = ?")
        params.append(total_parts_scraped)
    if not params:
        return
    conn.execute(
        f"UPDATE checkpoint SET {', '.join(updates)} WHERE id = 1",
        params,
    )


def get_checkpoint(conn: sqlite3.Connection) -> dict:
    row = conn.execute("SELECT * FROM checkpoint WHERE id = 1").fetchone()
    if not row:
        return {}
    return dict(row)


def insert_rockauto(conn: sqlite3.Connection, rows: list[dict]) -> int:
    """Insert scraped parts into rockauto table: year, make, model, engine, category, part_type, manufacturer, part_number, price, image_url, scraped_at."""
    if not rows:
        return 0
    cols = ["year", "make", "model", "engine", "category", "part_type", "manufacturer", "part_number", "price", "image_url", "scraped_at"]
    placeholders = ", ".join("?" * len(cols))
    col_names = ", ".join(cols)
    conn.executemany(
        f"INSERT INTO rockauto ({col_names}) VALUES ({placeholders})",
        [
            (
                r.get("year"), r.get("make"), r.get("model"), r.get("engine"),
                r.get("category"), r.get("part_type"), r.get("manufacturer"), r.get("part_number"),
                r.get("price") or r.get("listing_price"), r.get("image_url"),
                r.get("scraped_at") or datetime.now().isoformat(),
            )
            for r in rows
        ],
    )
    return len(rows)


def insert_product_urls(conn: sqlite3.Connection, rows: list[dict]) -> int:
    if not rows:
        return 0
    conn.executemany(
        """
        INSERT INTO product_urls (
            info_url, discovered_at, year, make, model, engine,
            category, part_type, manufacturer, part_number, listing_price
        )
        VALUES (?, datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(info_url) DO UPDATE SET
            year=excluded.year,
            make=excluded.make,
            model=excluded.model,
            engine=excluded.engine,
            category=excluded.category,
            part_type=excluded.part_type,
            manufacturer=excluded.manufacturer,
            part_number=excluded.part_number,
            listing_price=excluded.listing_price,
            status='pending',
            last_error=NULL,
            detail_scraped_at=NULL
        """,
        [
            (
                r.get("info_url"),
                r.get("year"),
                r.get("make"),
                r.get("model"),
                r.get("engine"),
                r.get("category"),
                r.get("part_type"),
                r.get("manufacturer"),
                r.get("part_number"),
                r.get("price"),
            )
            for r in rows
            if r.get("info_url")
        ],
    )
    return len(rows)


def fetch_product_urls(conn: sqlite3.Connection, status: str = "pending", limit: int = 100) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT *
        FROM product_urls
        WHERE status = ?
        ORDER BY id
        LIMIT ?
        """,
        (status, limit),
    ).fetchall()


def mark_product_urls_in_progress(conn: sqlite3.Connection, ids: list[int]) -> None:
    if not ids:
        return
    conn.executemany(
        "UPDATE product_urls SET status='in_progress', last_error=NULL WHERE id=?",
        [(i,) for i in ids],
    )


def mark_product_url_done(conn: sqlite3.Connection, url_id: int) -> None:
    conn.execute(
        "UPDATE product_urls SET status='done', detail_scraped_at=datetime('now'), last_error=NULL WHERE id=?",
        (url_id,),
    )


def mark_product_url_error(conn: sqlite3.Connection, url_id: int, error: str) -> None:
    conn.execute(
        "UPDATE product_urls SET status='error', last_error=?, detail_scraped_at=NULL WHERE id=?",
        (error[:500], url_id),
    )
