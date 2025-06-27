#%%
#!/usr/bin/env python3
from pathlib import Path
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

# ─── Configuration ─────────────────────────────────────────────────────────────
# Fixed path to your Windows project exports folder under WSL
EXPORT_DIR = Path("/mnt/c/DEprojects/xmd_jumia/orders")

# Postgres connection info
DB_CONFIG = {
   
}

# Target table name
TABLE_NAME = "all_orders"
# ────────────────────────────────────────────────────────────────────────────────

def find_latest_export_csv(export_dir: Path) -> Path:
    """
    Return the most recently modified 'orders_export-*.csv' file in export_dir.
    """
    if not export_dir.exists():
        raise FileNotFoundError(f"Export directory not found: {export_dir}")
    files = list(export_dir.glob('orders_export-*.csv'))
    if not files:
        raise FileNotFoundError(f"No files matching 'export-*.csv' in {export_dir}")
    return max(files, key=lambda p: p.stat().st_mtime)


def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [
        c.strip().lower().replace(' ', '_').replace('.', '')
        for c in df.columns
    ]
    return df


def ensure_table(cur, columns):
    """
    Create target table if it doesn't exist, then add any new columns.
    """
    # Base table DDL
    cur.execute(sql.SQL("""
        CREATE TABLE IF NOT EXISTS {table} (
            id         SERIAL PRIMARY KEY,
            loaded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """
    ).format(table=sql.Identifier(TABLE_NAME)))

    # Fetch existing columns
    cur.execute(sql.SQL("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = %s
    """), [TABLE_NAME])
    existing = {row[0] for row in cur.fetchall()}

    # Add missing columns
    for col in columns:
        if col not in existing:
            cur.execute(sql.SQL("""
                ALTER TABLE {table} ADD COLUMN {col} TEXT;
            """
            ).format(
                table=sql.Identifier(TABLE_NAME),
                col=sql.Identifier(col),
            ))


def bulk_insert(cur, df: pd.DataFrame):
    """
    Bulk-insert DataFrame rows into the target table.
    """
    cols = df.columns.tolist()
    insert_stmt = sql.SQL('INSERT INTO {table} ({fields}) VALUES %s').format(
        table=sql.Identifier(TABLE_NAME),
        fields=sql.SQL(', ').join(map(sql.Identifier, cols))
    )
    values = [tuple(str(v) if pd.notna(v) else None for v in row)
              for row in df[cols].itertuples(index=False)]
    execute_values(cur, insert_stmt, values)


def main():
    # 1) Find & read latest CSV from fixed path
    latest_file = find_latest_export_csv(EXPORT_DIR)
    print(f"Loading file: {latest_file}")
    df = pd.read_csv(latest_file)
    df = normalize_cols(df)
    print("Normalized columns:", df.columns.tolist())

    # 2) Connect to Postgres
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()

    # 3) Ensure table & schema
    ensure_table(cur, df.columns)

    # 4) Insert data
    bulk_insert(cur, df)

    # 5) Commit & close
    conn.commit()
    cur.close()
    conn.close()
    print("ETL complete.")

if __name__ == '__main__':
    main()

# %%

