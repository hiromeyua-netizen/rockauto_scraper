"""
Quick script to print current RockAuto scraping progress (checkpoint + row count).
Usage: python checkpoint_viewer.py
"""
try:
    from rockauto.models import get_connection, init_db
except ImportError:
    from models import get_connection, init_db

def main():
    init_db()
    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM checkpoint WHERE id = 1").fetchone()
        if row:
            print("Checkpoint:")
            keys = row.keys() if hasattr(row, "keys") else [f"column_{i}" for i in range(len(row))]
            for k in keys:
                print(f"  {k}: {row[k]}")
        count = conn.execute("SELECT COUNT(*) FROM parts").fetchone()[0]
        print(f"Total parts in DB: {count}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
