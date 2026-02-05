# RockAuto Scraper

Async Playwright scraper for [RockAuto](https://www.rockauto.com) parts catalog. Writes to SQLite (`rockauto.db`) with checkpoint/resume; export to CSV via `export.py`.

## Requirements

- Python 3.10+
- Ubuntu 24.04 (or Windows/macOS for local dev)

## Setup

```bash
cd rockauto
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
```

Optional: copy `.env` from repo root or create one in `rockauto/` with Bright Data proxy:

```env
BRIGHTDATA_PROXY_SERVER=brd.superproxy.io:33335
BRIGHTDATA_PROXY_USER=your-username
BRIGHTDATA_PROXY_PASS=your-password
```

## Vehicles input

- **Option A – CSV**: put `vehicles.csv` in the `rockauto` folder with columns: `year`, `make`, `model` (optional: `engine_slug`, `engine_name`). Example:

  ```csv
  year,make,model
  2020,Toyota,Camry
  2019,Honda,Accord
  ```

- **Option B – No CSV**: scraper discovers the full catalog (Make → Year → Model) from the site and scrapes all. Resume is supported from the checkpoint.

## Run

From project root:

```bash
python -m rockauto.scraper
```

Or from `rockauto` directory:

```bash
python scraper.py
```

## Behaviour

- **Bandwidth**: Blocks images, fonts, CSS, media via `route.abort()`; logs bandwidth every 100 pages.
- **Checkpoint**: Progress (last make/year/model/engine, total parts) is saved in SQLite; on restart, scraping resumes from the checkpoint.
- **Soft blocks**: Detects infinite loading / empty results; recycles context and retries.
- **Context recycling**: Each worker recycles its browser context after 150 pages or 25 minutes to avoid memory growth.
- **SIGTERM**: Saves checkpoint and exits cleanly.

## Export to CSV

```bash
python export.py --output rockauto_export.csv
python export.py --since 2025-01-01 --make Toyota -o toyota.csv
```

Options: `--since`, `--until`, `--make`, `--year`, `--model`.

## Check progress

```bash
python checkpoint_viewer.py
```

## Output schema (SQLite / CSV)

| Column       | Description        |
|-------------|--------------------|
| year        | Vehicle year       |
| make        | Make               |
| model       | Model              |
| engine      | Engine (if any)    |
| category    | Part category      |
| part_type   | Description        |
| manufacturer| Brand              |
| part_number | Part number        |
| price       | Price text         |
| scraped_at  | UTC timestamp      |

## Config (env)

- `ROCKAUTO_MAX_CONTEXTS` – concurrent workers (default 10)
- `ROCKAUTO_CONTEXT_MAX_PAGES` – recycle context after N pages (default 150)
- `ROCKAUTO_CONTEXT_MAX_MINUTES` – recycle after N minutes (default 25)
- `ROCKAUTO_PAGE_TIMEOUT` – page load timeout ms (default 45000)
- `ROCKAUTO_MAX_RETRIES` – retries per page (default 3)
- `BRIGHTDATA_PROXY_*` or `PROXY_*` – proxy credentials
