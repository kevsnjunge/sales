import sys
import numpy as np
import os
import pandas as pd
import psycopg2
import time
import logging

from psycopg2.extras import execute_values
# from dotenv import load_dotenv

# load_dotenv()

# ------------------------
# ENV VARIABLES
# ------------------------
RS_HOST = os.getenv("RS_HOST")
RS_DB   = os.getenv("RS_DB")
RS_USER = os.getenv("RS_USER")
RS_PW   = os.getenv("RS_PW")
RS_PORT = os.getenv("RS_PORT")

FULL_TABLE = os.getenv("FULL_TABLE", "sales.monthly_customer_groupings")

EXCEL_PATH = os.getenv(
    "EXCEL_PATH",
    r"D:\Redshift Data Push\Baking and Allied\Sales\Exports\monthly_customer_groupings.xlsx"
)

# ------------------------
# LOGGING
# ------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ------------------------
# SCHEMA
# ------------------------
EXPECTED_COLUMNS = [
    "month",
    "branch",
    "category",
    "channel",
    "company",
    "converted_quantity",
    "crates",
    "customer_code",
    "customer_name",
    "daily_average",
    "days_active",
    "gross_sales",
    "quantity",
    "region",
    "route",
    "sub_region",
    "end_month"
]

NUMERIC_COLS = [
    "converted_quantity",
    "crates",
    "daily_average",
    "days_active",
    "gross_sales",
    "quantity"
]

# ------------------------
# EXTRACT
# ------------------------
def read_excel_robust(path):
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    df = pd.read_excel(path)
    logger.info("Loaded %d rows", len(df))
    return df

# ------------------------
# TRANSFORM
# ------------------------
def clean_dataframe(df):

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"[\s\-]+", "_", regex=True)
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )

    df = df.rename(columns={
        "endmonth": "end_month",
        "customercode": "customer_code",
        "customername": "customer_name"
    })

    df = df.loc[:, ~df.columns.duplicated()]

    text_cols = df.select_dtypes(include=["object", "string"]).columns

    for col in text_cols:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace("\u00A0", " ", regex=False)
            .str.strip()
            .replace("nan", None)
        )

    return df.replace({np.nan: None})

# ------------------------
def enforce_data_types(df):

    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "end_month" in df.columns:
        df["end_month"] = pd.to_datetime(df["end_month"], errors="coerce").dt.date

    return df

# ------------------------
def enforce_schema(df):

    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            logger.warning("Missing column added as NULL: %s", col)
            df[col] = None

    return df[EXPECTED_COLUMNS]

# ------------------------
def add_column_if_missing(cur, table, column, col_type):

    if "." in table:
        schema, tbl = table.split(".")
    else:
        schema = "public"
        tbl = table

    cur.execute("""
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema=%s
        AND table_name=%s
        AND column_name=%s
    """, (schema, tbl, column))

    if cur.fetchone()[0] == 0:
        cur.execute(f'ALTER TABLE {table} ADD COLUMN "{column}" {col_type}')
        logger.info("Added column: %s", column)

# ------------------------
# MAIN
# ------------------------
def main():

    start = time.time()
    logger.info("ETL started")

    global FULL_TABLE
    if "." in FULL_TABLE:
        schema = FULL_TABLE.split(".")[0]
    else:
        schema = "public"
        FULL_TABLE = f"{schema}.{FULL_TABLE}"

    # EXTRACT
    df = read_excel_robust(EXCEL_PATH)

    # TRANSFORM
    df = clean_dataframe(df)
    df = enforce_data_types(df)
    df = enforce_schema(df)

    if df.empty:
        logger.warning("No data found")
        return

    rows = [tuple(r) for r in df.to_numpy()]

    cols_sql = ", ".join([f'"{c}"' for c in df.columns])
    insert_sql = f"INSERT INTO {FULL_TABLE} ({cols_sql}) VALUES %s"

    # CONNECT
    conn = psycopg2.connect(
        host=RS_HOST,
        dbname=RS_DB,
        user=RS_USER,
        password=RS_PW,
        port=RS_PORT
    )

    try:
        conn.autocommit = False
        cur = conn.cursor()

        # CREATE SCHEMA
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        # CREATE TABLE
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {FULL_TABLE} (
                month               VARCHAR(50),
                branch              VARCHAR(50),
                category            VARCHAR(100),
                channel             VARCHAR(200),
                company             VARCHAR(50),
                converted_quantity  DOUBLE PRECISION,
                crates              DOUBLE PRECISION,
                customer_code       VARCHAR(50),
                customer_name       VARCHAR(100),
                daily_average       DOUBLE PRECISION,
                days_active         DOUBLE PRECISION,
                gross_sales         DOUBLE PRECISION,
                quantity            DOUBLE PRECISION,
                region              VARCHAR(255),
                route               VARCHAR(50),
                sub_region          VARCHAR(100),
                end_month           DATE
            )
        """)

        # AUTO COLUMN FIX
        for col in EXPECTED_COLUMNS:
            if col == "end_month":
                add_column_if_missing(cur, FULL_TABLE, col, "DATE")
            elif col in NUMERIC_COLS:
                add_column_if_missing(cur, FULL_TABLE, col, "FLOAT8")
            else:
                add_column_if_missing(cur, FULL_TABLE, col, "VARCHAR(255)")

        # ------------------------
        # DELETE ONLY AFFECTED MONTHS
        # ------------------------
        unique_months = df["end_month"].dropna().unique().tolist()

        if unique_months:
            cur.execute(
                f"DELETE FROM {FULL_TABLE} WHERE end_month = ANY(%s)",
                (unique_months,)
            )
            logger.info("Deleted data for end_month(s): %s", unique_months)

        # ------------------------
        # INSERT NEW DATA
        # ------------------------
        execute_values(cur, insert_sql, rows, page_size=2000)

        conn.commit()
        logger.info("Inserted %d rows", len(rows))

    except Exception:
        conn.rollback()
        logger.exception("ETL FAILED")
        raise

    finally:
        conn.close()

    logger.info("ETL completed in %.2f seconds", time.time() - start)


if __name__ == "__main__":
    main()