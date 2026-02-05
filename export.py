"""
Export RockAuto SQLite (rockauto.db) to CSV with optional filters.
Usage:
  python export.py [--output FILE] [--since DATE] [--make MAKE] [--year YEAR]
  python export.py --product-urls [--output product_urls.csv]  # Export product Info URLs only
"""
import argparse
import csv
from pathlib import Path

try:
    from rockauto.models import get_connection, DB_PATH
except ImportError:
    from models import get_connection, DB_PATH


def export_product_urls(output_path: Path, **filters) -> int:
    """Export only product Info URLs (from ra-btn-moreinfo links) to a separate CSV."""
    conn = get_connection()
    try:
        query = """
            SELECT info_url, year, make, model, engine, category, part_type, manufacturer, part_number, price, scraped_at
            FROM parts WHERE info_url IS NOT NULL AND info_url != ''
        """
        params = []
        if filters.get("since"):
            query += " AND date(scraped_at) >= date(?)"
            params.append(filters["since"])
        if filters.get("until"):
            query += " AND date(scraped_at) <= date(?)"
            params.append(filters["until"])
        if filters.get("make"):
            query += " AND make = ?"
            params.append(filters["make"])
        if filters.get("year"):
            query += " AND year = ?"
            params.append(filters["year"])
        if filters.get("model"):
            query += " AND model = ?"
            params.append(filters["model"])
        query += " ORDER BY scraped_at, id"
        cur = conn.execute(query, params)
        rows = cur.fetchall()
        if not rows:
            return 0
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["info_url", "year", "make", "model", "engine", "category", "part_type", "manufacturer", "part_number", "price", "scraped_at"])
            for row in rows:
                writer.writerow(list(row))
        return len(rows)
    finally:
        conn.close()


def export_csv(
    output_path: Path,
    since: str | None = None,
    until: str | None = None,
    make: str | None = None,
    year: str | None = None,
    model: str | None = None,
) -> int:
    conn = get_connection()
    try:
        query = "SELECT year, make, model, engine, category, part_type, manufacturer, part_number, price, info_url, scraped_at FROM parts WHERE 1=1"
        params = []
        if since:
            query += " AND date(scraped_at) >= date(?)"
            params.append(since)
        if until:
            query += " AND date(scraped_at) <= date(?)"
            params.append(until)
        if make:
            query += " AND make = ?"
            params.append(make)
        if year:
            query += " AND year = ?"
            params.append(year)
        if model:
            query += " AND model = ?"
            params.append(model)
        query += " ORDER BY scraped_at, id"
        cur = conn.execute(query, params)
        rows = cur.fetchall()
        if not rows:
            return 0
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["year", "make", "model", "engine", "category", "part_type", "manufacturer", "part_number", "price", "info_url", "scraped_at"])
            for row in rows:
                writer.writerow(list(row))
        return len(rows)
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser(description="Export RockAuto SQLite to CSV")
    ap.add_argument("--output", "-o", help="Output CSV path")
    ap.add_argument("--product-urls", "-p", action="store_true", help="Export product Info URLs to separate CSV")
    ap.add_argument("--since", help="Only rows scraped on or after this date (YYYY-MM-DD)")
    ap.add_argument("--until", help="Only rows scraped on or before this date (YYYY-MM-DD)")
    ap.add_argument("--make", help="Filter by make")
    ap.add_argument("--year", help="Filter by year")
    ap.add_argument("--model", help="Filter by model")
    args = ap.parse_args()
    filters = {"since": args.since, "until": args.until, "make": args.make, "year": args.year, "model": args.model}
    if args.product_urls:
        out = Path(args.output or "product_urls.csv")
        n = export_product_urls(out, **filters)
        print(f"Exported {n} product URLs to {out}")
    else:
        out = Path(args.output or "rockauto_export.csv")
        n = export_csv(out, **filters)
        print(f"Exported {n} rows to {out}")


if __name__ == "__main__":
    main()
