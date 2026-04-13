# Branch Daily Sales ETL Pipeline

This project implements an **ETL (Extract, Transform, Load) pipeline** that processes branch-level sales data and loads it into an Amazon Redshift table.

The pipeline extracts data from an API (with CSV fallback), cleans and standardizes it using Python, and ensures it is stored in a structured and consistent format for analytics and reporting.

##  Features

- Extracts data from:
  - API (`acl_py_util`)
  - CSV fallback (auto-detected via logs)
- Cleans and standardizes column names
- Handles encoding issues automatically
- Applies column alias mapping
- Converts:
  - Dates → datetime
  - Numeric fields → floats
- Enforces schema consistency
- Automatically:
  - Creates schema if not exists
  - Creates table if not exists
  - Adds missing columns dynamically
- Bulk inserts data into Redshift
- Includes logging and error handling

---

## Data Schema

### Expected Input Columns
- branch
- company
- converted_quantity
- country
- posting_date
- quantity
- region
- sub_region
- Total_Amount_KSH
- Total_Amount_USD
- Total_Amount_UGX
- Total_Amount_TSH

### Final Redshift Table Schema
- branch VARCHAR(255)
- company VARCHAR(255)
- converted_quantity DOUBLE PRECISION
- country VARCHAR(100)
- posting_date TIMESTAMP
- quantity DOUBLE PRECISION
- region VARCHAR(255)
- sub_region VARCHAR(255)
- total_amount_ksh DOUBLE PRECISION
- total_amount_usd DOUBLE PRECISION
- total_amount_ugx DOUBLE PRECISION
- total_amount_tsh DOUBLE PRECISION

## Configuration
Create a `.env` file in your project root:
- RS_HOST=your_redshift_host
- RS_DB=your_database
- RS_USER=your_username
- RS_PW=your_password
- RS_PORT=5439

- FULL_TABLE=sales.branch_daily_sales


 **Important:**  
Make sure `.env` is included in `.gitignore` so it is NOT pushed to GitHub.


## Installation

Install required dependencies:
- pip install pandas 
- psycopg2-binary 
- python-dotenv 
- numpy




## Usage

Run the pipeline:
 - branch_daily_sales.py



##  How It Works

### 1. Extract
- Attempts to fetch data using `acl_py_util`
- Falls back to CSV if API fails

### 2. Transform
- Normalizes column names (lowercase, underscores)
- Applies alias mappings (e.g. `totalamountksh → total_amount_ksh`)
- Cleans text fields
- Converts:
  - Dates → datetime
  - Numeric fields → float
- Ensures required schema is present

### 3. Load
- Connects to Redshift
- Creates schema/table if missing
- Adds missing columns dynamically
- Truncates table (full refresh)
- Inserts data in bulk



## Important Notes

### Full Table Refresh

## ALTER TABLE sales.branch_daily_sales
- RENAME COLUMN 
  - old_column TO new_column;




## Common Issues

### Missing Environment Variables
- Ensure `.env` file exists
- Restart VS Code if variables are not loaded

### CSV Not Found
- Script waits up to 60 seconds for file generation

### Encoding Errors
- Automatically handled (UTF-8, Latin1 fallback)

### Column Mismatch
- Missing columns are filled with NULL
- Check logs for warnings



##  Future Improvements

- Incremental loading instead of truncate
- Data validation checks
- Logging to file or monitoring system
- Scheduled execution (cron / Airflow)
- Error notifications (Slack / Email)

##  Author

Kelvin Njunge

##  License

This project is for internal/business use. Update licensing as needed.



