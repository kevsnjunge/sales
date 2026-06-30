import os
import time
import re
import numpy as np
import pandas as pd
import logging
import psycopg2

from psycopg2.extras import execute_values
# from dotenv import load_dotenv
from acl_py_util import acl_py_util, logger

# load_dotenv()

# -------------------------
# CONNECTION
# -------------------------
RS_HOST = os.getenv("RS_HOST")
RS_DB = os.getenv("RS_DB")
RS_USER = os.getenv("RS_USER")
RS_PW = os.getenv("RS_PW")
RS_PORT = os.getenv("RS_PORT")


FULL_TABLE = os.getenv(
    "FULL_TABLE",
    "sales.customer_route_daily_sales"
)

# -------------------------
# EXPECTED SCHEMA
# -------------------------
EXPECTED_COLUMNS = [
    "posting_date",
    "company",
    "customer_status",
    "country",
    "channel",
    "region",
    "sub_region",
    "branch",
    "route",
    "card_code",
    "card_name",
    "converted_quantity",
    "quantity",
    "usd_amount",
    "ksh_amount",
    "ugx_amount",
    "tzs_amount"
]

# -------------------------
# COLUMN TYPES
# -------------------------
COLUMN_TYPES = {

    "posting_date": "TIMESTAMP",

    "company": "VARCHAR(50)",

    "customer_status": "VARCHAR(50)",

    "country": "VARCHAR(50)",

    "channel": "VARCHAR(200)",

    "region": "VARCHAR(255)",

    "sub_region": "VARCHAR(100)",

    "branch": "VARCHAR(50)",

    "route": "VARCHAR(50)",

    "card_code": "VARCHAR(50)",

    "card_name": "VARCHAR(100)",

    "converted_quantity": "DOUBLE PRECISION",

    "quantity": "DOUBLE PRECISION",

    "usd_amount": "DOUBLE PRECISION",

    "ksh_amount": "DOUBLE PRECISION",

    "ugx_amount": "DOUBLE PRECISION",

    "tzs_amount": "DOUBLE PRECISION",
}

# -------------------------
# COLUMN ALIASES
# -------------------------
COLUMN_ALIASES = {

    "postingdate": "posting_date",

    "post_date": "posting_date",

    "cardcode": "card_code",

    "cardname": "card_name",

    "convertedquantity": "converted_quantity",

    "customerstatus": "customer_status",

    "usdamount": "usd_amount",

    "kshamount": "ksh_amount",

    "ugxamount": "ugx_amount",

    "tzsamount": "tzs_amount"
}

# -------------------------
# CSV PATH CATCHER
# -------------------------

class CsvPathCatcher(logging.Handler):
    def __init__(self):
        super().__init__()
        self.csv_path = None

    def emit(self, record):
        try:
            msg = record.getMessage()
            if "Reading CSV file:" in msg:
                path = msg.split("Reading CSV file:")[-1].strip()
                self.csv_path = path

                # Fix encoding RIGHT HERE before acl_py_util reads it
                self._fix_encoding(path)

        except Exception:
            pass

    def _fix_encoding(self, path):
        for enc in ["cp1252", "latin1", "utf-8-sig"]:
            try:
                with open(path, "r", encoding=enc) as f:
                    content = f.read()
                with open(path, "w", encoding="utf-8") as f:
                    f.write(content)
                logger.info(f"Re-encoded {path} from {enc} to utf-8")
                return
            except UnicodeDecodeError:
                continue
class SuppressUtf8Warning(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        return "utf-8" not in msg.lower()



def read_csv_robust(path):
    encodings = ["utf-8-sig", "cp1252", "latin1", "utf-8"]
    
    for enc in encodings:
        try:
            df = pd.read_csv(path, encoding=enc)
            logger.info(f"Successfully read CSV with encoding: {enc}")
            return df
        except UnicodeDecodeError as e:
            logger.warning(f"Encoding {enc} failed: {e}")
            continue
        except Exception as e:
            logger.error(f"Unexpected error reading CSV: {e}")
            raise  # don't silently swallow non-encoding errors

    # Final fallback
    logger.warning("All encodings failed, reading with errors='replace'")
    return pd.read_csv(path, encoding="latin1", errors="replace")

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


# -------------------------
# TRANSFORM
# -------------------------
def clean_dataframe(df):

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    df = df.rename(
        columns=COLUMN_ALIASES
    )

    # remove duplicate columns
    df = df.loc[
        :,
        ~df.columns.duplicated()
    ]

    # clean text columns
    text_cols = df.select_dtypes(
        include=["object", "string"]
    ).columns

    for col in text_cols:

        df[col] = (
            df[col]
            .astype("string")
            .str.strip()
            .str.replace("\u00A0", " ")
        )

    return df.replace({np.nan: None})


def enforce_types(df):

    # posting date
    if "posting_date" in df.columns:

        df["posting_date"] = pd.to_datetime(
            df["posting_date"],
            errors="coerce"
        )

    # numeric columns
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

            logger.warning(
                "Missing column: %s → filled NULL",
                col
            )

            df[col] = None

    return df[EXPECTED_COLUMNS]

# -------------------------
# DB HELPER
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
        AND column_name = '{column}';
    """)

    exists = cur.fetchone()[0]

    if exists == 0:

        cur.execute(f'''
            ALTER TABLE {table}
            ADD COLUMN "{column}" {col_type};
        ''')

        logger.info(
            "Added column: %s",
            column
        )

# -------------------------
# MAIN PIPELINE
# DELETE + APPEND
# -------------------------
def main():

    start = time.time()

   # Catcher now fixes encoding before acl_py_util reads the file
    catcher = CsvPathCatcher()
    logging.getLogger().addHandler(catcher)

    df = None
    try:
        df = acl_py_util.from_an()
    except Exception as e:
        logger.warning(f"ACL failed, falling back to CSV: {e}")

    if df is None or df.empty:
        if catcher.csv_path:
            df = read_csv_robust(catcher.csv_path)
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

        logger.warning(
            "Empty dataframe"
        )

        return

    # remove duplicates inside dataframe
    df = df.drop_duplicates()

    logger.info(
        "Rows ready: %s",
        len(df)
    )

    # -------------------------
    # PREPARE INSERT
    # -------------------------
    rows = [
        tuple(r)
        for r in df.to_numpy()
    ]

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

            # create schema
            cur.execute(
                f"CREATE SCHEMA IF NOT EXISTS {schema};"
            )

            # create table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {FULL_TABLE} (

                    posting_date TIMESTAMP,

                    company VARCHAR(50),

                    customer_status VARCHAR(50),

                    country VARCHAR(50),

                    channel VARCHAR(200),

                    region VARCHAR(255),

                    sub_region VARCHAR(100),

                    branch VARCHAR(50),

                    route VARCHAR(50),

                    card_code VARCHAR(50),

                    card_name VARCHAR(100),

                    converted_quantity DOUBLE PRECISION,

                    quantity DOUBLE PRECISION,

                    usd_amount DOUBLE PRECISION,

                    ksh_amount DOUBLE PRECISION,

                    ugx_amount DOUBLE PRECISION,

                    tzs_amount DOUBLE PRECISION

                );
            """)

            # -------------------------
            # AUTO SCHEMA EVOLUTION
            # -------------------------
            for col, col_type in COLUMN_TYPES.items():

                add_column_if_missing(
                    cur,
                    FULL_TABLE,
                    col,
                    col_type
                )

            # -------------------------
            # DELETE EXISTING DATES
            # -------------------------
            posting_dates = (
                df["posting_date"]
                .dropna()
                .dt.date
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
                    WHERE CAST(posting_date AS DATE)
                    IN ({date_list_sql})
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
            "Load successful"
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



