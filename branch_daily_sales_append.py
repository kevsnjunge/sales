import os
import time
import re
import numpy as np
import pandas as pd
import logging
import psycopg2

from psycopg2.extras import execute_values
#from dotenv import load_dotenv
from acl_py_util import acl_py_util, logger

# -------------------------
# ENVIRONMENT VARIABLES
# -------------------------
#load_dotenv()

RS_HOST = os.getenv("RS_HOST")
RS_DB = os.getenv("RS_DB")
RS_USER = os.getenv("RS_USER")
RS_PW = os.getenv("RS_PW")
RS_PORT = os.getenv("RS_PORT")


FULL_TABLE = os.getenv("FULL_TABLE", "sales.branch_daily_sales")

# -------------------------
# EXPECTED COLUMNS
# -------------------------
EXPECTED_COLUMNS = [
    "branch",
    "company",
    "converted_quantity",
    "country",
    "posting_date",
    "quantity",
    "region",
    "sub_region",
    "usd_amount",
    "ksh_amount",
    "ugx_amount",
    "tzs_amount",
]

COLUMN_ALIASES = {
    "postingdate": "posting_date",
    "post_date": "posting_date",
    "usdamount": "usd_amount",
    "kshamount": "ksh_amount",
    "ugxamount": "ugx_amount",
    "tzsamount": "tzs_amount",
}

# -------------------------
# CSV PATH CATCHER
# -------------------------
class CsvPathCatcher(logging.Handler):

    def __init__(self):
        super().__init__()

        self.csv_path = None

        self.patterns = [
            re.compile(
                r"Reading CSV file:\s*[\"']?(.*?\.csv)[\"']?$",
                re.IGNORECASE
            ),
            re.compile(
                r"Writing CSV file:\s*[\"']?(.*?\.csv)[\"']?$",
                re.IGNORECASE
            ),
        ]

    def emit(self, record):

        try:

            msg = record.getMessage()

            for pat in self.patterns:

                m = pat.search(msg)

                if m:
                    self.csv_path = m.group(1).strip()

        except Exception:
            pass

# -------------------------
# FILE HELPERS
# -------------------------
def wait_for_file(path, timeout=60):

    if not path:
        return False

    start = time.time()

    while time.time() - start < timeout:

        if os.path.exists(path) and os.path.getsize(path) > 0:
            return True

        time.sleep(0.25)

    return False


def read_csv_robust(path):

    for enc in ["utf-8", "utf-8-sig", "cp1252", "latin1"]:

        try:
            return pd.read_csv(path, encoding=enc)

        except UnicodeDecodeError:
            continue

    return pd.read_csv(
        path,
        encoding="utf-8",
        errors="replace"
    )

# -------------------------
# TRANSFORM
# -------------------------
def clean_dataframe(df):

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
    )

    df = df.rename(columns=COLUMN_ALIASES)

    # remove duplicate columns
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()]

    # clean text columns
    text_cols = df.select_dtypes(
        include=["object", "string"]
    ).columns

    for col in text_cols:

        df[col] = (
            df[col]
            .astype("string")
            .str.replace("\u00A0", " ")
            .str.strip()
        )

    return df.replace({np.nan: None})


def enforce_types(df):

    # date
    if "posting_date" in df.columns:

        df["posting_date"] = pd.to_datetime(
            df["posting_date"],
            errors="coerce"
        ).dt.date

    # numeric
    numeric_cols = [
        "converted_quantity",
        "quantity",
        "usd_amount",
        "ksh_amount",
        "ugx_amount",
        "tzs_amount"
    ]

    for col in numeric_cols:

        if col in df.columns:

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

    return df


def enforce_schema(df):

    for col in EXPECTED_COLUMNS:

        if col not in df.columns:
            df[col] = None

    return df[EXPECTED_COLUMNS]

# -------------------------
# DB HELPERS
# -------------------------
def add_column_if_missing(
    cur,
    table,
    column,
    col_type
):

    schema, tbl = table.split(".")

    cur.execute(f"""
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
        AND table_name = '{tbl}'
        AND column_name = '{column}'
    """)

    exists = cur.fetchone()[0]

    if exists == 0:

        cur.execute(
            f'''
            ALTER TABLE {table}
            ADD COLUMN "{column}" {col_type}
            '''
        )

        logger.info(
            "Added column: %s",
            column
        )

# -------------------------
# MAIN PIPELINE
# -------------------------
def main():

    start = time.time()

    catcher = CsvPathCatcher()

    logging.getLogger().addHandler(catcher)

    logging.getLogger(
        "acl_py_util"
    ).addHandler(catcher)

    logger.info(
        "Starting ETL (DELETE + APPEND MODE)..."
    )

    df = None

    # -------------------------
    # EXTRACT
    # -------------------------
    try:

        df = acl_py_util.from_an()

    except Exception:

        logger.exception(
            "ACL extraction failed"
        )

    if df is None or getattr(df, "empty", True):

        if catcher.csv_path and wait_for_file(catcher.csv_path):

            df = read_csv_robust(
                catcher.csv_path
            )

        else:

            logger.error("No data found")

            return

    # -------------------------
    # TRANSFORM
    # -------------------------
    df = clean_dataframe(df)

    df = enforce_types(df)

    df = enforce_schema(df)

    if df.empty:

        logger.warning("Empty dataframe")

        return

    # remove duplicate rows from incoming file
    df = df.drop_duplicates()

    # -------------------------
    # PREPARE INSERT
    # -------------------------
    rows = list(
        df.itertuples(
            index=False,
            name=None
        )
    )

    cols_sql = ",".join(
        [f'"{c}"' for c in EXPECTED_COLUMNS]
    )

    insert_sql = f"""
        INSERT INTO {FULL_TABLE}
        ({cols_sql})
        VALUES %s
    """

    # -------------------------
    # CONNECT
    # -------------------------
    conn = psycopg2.connect(
        host=RS_HOST,
        dbname=RS_DB,
        user=RS_USER,
        password=RS_PW,
        port=RS_PORT,
    )

    try:

        conn.autocommit = False

        with conn.cursor() as cur:

            schema = FULL_TABLE.split(".")[0]

            # -------------------------
            # CREATE SCHEMA
            # -------------------------
            cur.execute(
                f"CREATE SCHEMA IF NOT EXISTS {schema};"
            )

            # -------------------------
            # CREATE TABLE
            # -------------------------
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {FULL_TABLE} (

                    branch VARCHAR(255),

                    company VARCHAR(255),

                    converted_quantity DOUBLE PRECISION,

                    country VARCHAR(100),

                    posting_date DATE,

                    quantity DOUBLE PRECISION,

                    region VARCHAR(255),

                    sub_region VARCHAR(255),

                    usd_amount DOUBLE PRECISION,

                    ksh_amount DOUBLE PRECISION,

                    ugx_amount DOUBLE PRECISION,

                    tzs_amount DOUBLE PRECISION

                );
            """)

            # -------------------------
            # SCHEMA EVOLUTION
            # -------------------------
            numeric_cols = [
                "converted_quantity",
                "quantity",
                "usd_amount",
                "ksh_amount",
                "ugx_amount",
                "tzs_amount"
            ]

            for col in EXPECTED_COLUMNS:

                if col == "posting_date":

                    add_column_if_missing(
                        cur,
                        FULL_TABLE,
                        col,
                        "DATE"
                    )

                elif col in numeric_cols:

                    add_column_if_missing(
                        cur,
                        FULL_TABLE,
                        col,
                        "DOUBLE PRECISION"
                    )

                else:

                    add_column_if_missing(
                        cur,
                        FULL_TABLE,
                        col,
                        "VARCHAR(255)"
                    )

            # -------------------------
            # DELETE EXISTING DATES
            # -------------------------
            posting_dates = (
                df["posting_date"]
                .dropna()
                .astype(str)
                .unique()
                .tolist()
            )

            if posting_dates:

                date_list_sql = ",".join(
                    [f"'{d}'" for d in posting_dates]
                )

                delete_sql = f"""
                    DELETE FROM {FULL_TABLE}
                    WHERE posting_date IN ({date_list_sql})
                """

                logger.info(
                    "Deleting existing rows for %d posting dates",
                    len(posting_dates)
                )

                cur.execute(delete_sql)

            # -------------------------
            # INSERT FRESH DATA
            # -------------------------
            logger.info(
                "Inserting fresh data..."
            )

            execute_values(
                cur,
                insert_sql,
                rows,
                page_size=2000
            )

        conn.commit()

        logger.info(
            "Load successful. Inserted %d rows",
            len(rows)
        )

    except Exception:

        conn.rollback()

        logger.exception(
            "Load failed"
        )

        raise

    finally:

        conn.close()

    logger.info(
        "Completed in %.2fs",
        time.time() - start
    )

# -------------------------
# ENTRY POINT
# -------------------------
if __name__ == "__main__":

    main()








