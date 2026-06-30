#Importing 
import pandas as pd 
import os
import sys
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from acl_py_util import acl_py_util, logger
load_dotenv()

RS_HOST = os.getenv("RS_HOST")   
RS_DB  = os.getenv("RS_DB")
RS_USER  = os.getenv("RS_USER")
RS_PW  = os.getenv("RS_PW")  
RS_PORT =  os.getenv("RS_PORT") 

FULL_TABLE = os.getenv("FULL_TABLE", "sales.branch_daily_sales")

 

EXPECTED_COLUMNS = [
       "branch",
       "company",
       "converted_quantity",
       "country",
       "posting_date",
       "quantity",
       "region",
       "sub_region",
       "total_amount_ksh", 
       "total_amount_usd",
       "total_amount_ugx",
       "total_amount_tzs"]


COLUMN_ALIASES = {
    "postingdate": "posting_date",
    "post_date": "posting_date",
    "total_amount_usd":  "total_amountusd",
    "total_amountusd" : "total_amount_usd",
    "totalamountksh": "total_amount_ksh",
    "totalamountugx": "total_amount_ugx",
    "totalamounttzs":"total_amount_tzs"

}

NEW_COLUMNS = [
    ("total_amount_usd", "DOUBLE PRECISION"),
    ("total_amount_ugx", "DOUBLE PRECISION"),
    ("total_amount_tsh", "DOUBLE PRECISION"),
]


class CsvPathCatcher(logging.Handler):
    def __init__(self):
        super().__init__()
        self.csv_path = None
        self.patterns = [
            re.compile(r"Reading CSV file:\s*[\"']?(.*?\.csv)[\"']?$", re.IGNORECASE),
            re.compile(r"Writing CSV file:\s*[\"']?(.*?\.csv)[\"']?$", re.IGNORECASE),
        ]

    def emit(self, record):
        try:
            msg = record.getMessage()
            for pat in self.patterns:
                m = pat.search(msg)
                if m:
                    self.csv_path = m.group(1).strip()
                    break
        except Exception:
            pass


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
    return pd.read_csv(path, encoding="utf-8", errors="replace")


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    # Apply alias mapping
    df = df.rename(columns=COLUMN_ALIASES)

    # Clean text fields
    text_cols = df.select_dtypes(include=["object", "string"]).columns
    for col in text_cols:
        df[col] = (
            df[col]
            .astype("string")
            .str.replace("\u00A0", " ", regex=False)
            .str.strip()
        )

    return df.replace({np.nan: None})


def enforce_data_types(df: pd.DataFrame) -> pd.DataFrame:
    # Date
    if "posting_date" in df.columns:
        df["posting_date"] = pd.to_datetime(df["posting_date"], errors="coerce")

    # Numeric
    numeric_cols = [
        "converted_quantity",
        "quantity",
        "total_amount_ksh",
        "total_amount_usd",
        "total_amount_ugx", 
        "total_amount_tzs",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            logger.warning("Missing column: %s → filled with NULL", col)
            df[col] = None
    return df[EXPECTED_COLUMNS]


def add_column_if_missing(cur, table, column, col_type):
    schema, tbl = table.split(".")
    cur.execute(f"""
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
          AND table_name   = '{tbl}'
          AND column_name  = '{column}';
    """)
    if cur.fetchone()[0] == 0:
        cur.execute(f'ALTER TABLE {table} ADD COLUMN "{column}" {col_type};')
        logger.info("Added column: %s", column)

# ETL

def main():
    start = time.time()

    catcher = CsvPathCatcher()
    logging.getLogger().addHandler(catcher)
    logging.getLogger("acl_py_util").addHandler(catcher)

    logger.info("Starting ETL...")

    df = None
    try:
        df = acl_py_util.from_an()
    except Exception:
        logger.exception("ACL failed")

    if df is None or getattr(df, "empty", True):
        if catcher.csv_path and wait_for_file(catcher.csv_path):
            df = read_csv_robust(catcher.csv_path)
        else:
            logger.error("No data available")
            return

    # Transformations
    df = clean_dataframe(df)
    df = enforce_data_types(df)
    df = enforce_schema(df)

    if df.empty:
        logger.warning("Empty dataframe after processing")
        return

    logger.info("Rows ready: %s", len(df))

    # Prepare insert
    rows = [tuple(r) for r in df.to_numpy()]
    cols_sql = ",".join([f'"{c}"' for c in df.columns])
    insert_sql = f'INSERT INTO {FULL_TABLE} ({cols_sql}) VALUES %s'

    conn = None
    try:
        conn = psycopg2.connect(
            host=RS_HOST,
            dbname=RS_DB,
            user=RS_USER,
            password=RS_PW,
            port=RS_PORT,
        )
        conn.autocommit = False

        schema = FULL_TABLE.split(".")[0]

        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {FULL_TABLE} (
                    branch               VARCHAR(255),
                     company              VARCHAR(255),
                     converted_quantity  DOUBLE PRECISION ,
                     country              VARCHAR(100),
                     posting_date         TIMESTAMP,
                     quantity            DOUBLE PRECISION,
                     region               VARCHAR(255),
                     sub_region           VARCHAR(255),
                    total_amount_ksh    DOUBLE PRECISION  ,
                    total_amount_usd    DOUBLE PRECISION ,
                    total_amount_ugx    DOUBLE PRECISION ,
                    total_amount_tsh    DOUBLE PRECISION ,
                );
            """)

            for col, typ in NEW_COLUMNS:
                add_column_if_missing(cur, FULL_TABLE, col, typ)

            logger.info("Truncating table...")
            cur.execute(f"TRUNCATE TABLE {FULL_TABLE};")

            logger.info("Inserting data...")
            execute_values(cur, insert_sql, rows, page_size=2000)

        conn.commit()
        logger.info("Load successful")

    except Exception:
        if conn:
            conn.rollback()
        logger.exception("Load failed")
        raise

    finally:
        if conn:
            conn.close()

    logger.info("Completed in %.2fs", time.time() - start)


if __name__ == "__main__":
    main()