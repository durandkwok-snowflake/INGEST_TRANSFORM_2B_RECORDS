# INGEST_TRANSFORM_2B_RECORDS
INGEST_TRANSFORM_2B_RECORDS

# README: Ingest and Transform 2 Billion Records in Snowflake

This repository demonstrates how to efficiently ingest and transform a dataset containing over 2 billion records in Snowflake. Using the Medallion Architecture (Bronze, Silver, and Gold layers), the process leverages Snowflake's high performance for managing large datasets.

---

## Prerequisites

1. **Snowflake Account:** Ensure you have access to a Snowflake account.
2. **AWS S3 Bucket:** Data files should be stored in an S3 bucket.
3. **SnowSQL or Snowflake Web Interface:** Use these tools to execute the provided SQL scripts.
4. **Warehouse Setup:** Use a sufficiently large warehouse for optimal performance (e.g., `Large` size).

---

## Steps

### 1. Create Database and Schema
The following SQL initializes the database and schema for the ingestion process:
```sql
CREATE OR REPLACE DATABASE COMPANY_TB_TEST;
USE DATABASE COMPANY_TB_TEST;
CREATE SCHEMA BRONZE;
CREATE SCHEMA SILVER;
CREATE SCHEMA GOLD;
USE SCHEMA BRONZE;
```

### 2. Create the Target Table
Define the schema for the target table:
```sql
CREATE OR REPLACE TABLE billion_records_table1 (
    SS_SOLD_DATE_SK NUMBER(38,0),
    SS_SOLD_TIME_SK NUMBER(38,0),
    SS_ITEM_SK NUMBER(38,0),
    SS_CUSTOMER_SK NUMBER(38,0),
    SS_CDEMO_SK NUMBER(38,0),
    SS_HDEMO_SK NUMBER(38,0),
    SS_ADDR_SK NUMBER(38,0),
    SS_STORE_SK NUMBER(38,0),
    SS_PROMO_SK NUMBER(38,0),
    SS_TICKET_NUMBER NUMBER(38,0),
    SS_QUANTITY NUMBER(38,0),
    SS_WHOLESALE_COST NUMBER(7,2),
    SS_LIST_PRICE NUMBER(7,2),
    SS_SALES_PRICE NUMBER(7,2),
    SS_EXT_DISCOUNT_AMT NUMBER(7,2),
    SS_EXT_SALES_PRICE NUMBER(7,2),
    SS_EXT_WHOLESALE_COST NUMBER(7,2),
    SS_EXT_LIST_PRICE NUMBER(7,2),
    SS_EXT_TAX NUMBER(7,2),
    SS_COUPON_AMT NUMBER(7,2),
    SS_NET_PAID NUMBER(7,2),
    SS_NET_PAID_INC_TAX NUMBER(7,2),
    SS_NET_PROFIT NUMBER(7,2)
);
```

### 3. Prepare Stages for File Ingestion
Define stages for both CSV and Parquet file formats:

#### CSV Stage
```sql
CREATE STAGE TB_STAGE URL = 's3://mybucket/mystage' CREDENTIALS = (
  aws_key_id = 'my_aws_key_id', aws_secret_key = 'my_aws_secret_key'
);
```

#### Parquet Stage
```sql
CREATE STAGE LEGACY_PARQUET URL = 's3://mybucket/mystage' CREDENTIALS = (
  aws_key_id = 'my_aws_key_id', aws_secret_key = 'my_aws_secret_key'
);
```

### 4. Ingest Data into the Bronze Layer

#### Option 1: Using CSV Files
```sql
COPY INTO "COMPANY_TB_TEST"."BRONZE"."BILLION_RECORDS_TABLE1"
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23
    FROM '@"COMPANY_TB_TEST"."TPCDS_SF10TCL"."TB_STAGE"'
)
PATTERN='.*data_0_[0-4]_.*\.csv\.gz'
FORCE=TRUE
ON_ERROR=ABORT_STATEMENT;
```

#### Option 2: Using Parquet Files
Define the file format and ingest data:
```sql
CREATE OR REPLACE FILE FORMAT my_parquet_format2
  TYPE = PARQUET
  USE_VECTORIZED_SCANNER = TRUE;

COPY INTO "COMPANY_TB_TEST"."BRONZE"."BILLION_RECORDS_TABLE1"
FROM (
    SELECT $1:_COL_0::NUMBER(38, 0), $1:_COL_1::NUMBER(38, 0), $1:_COL_2::NUMBER(38, 0), ...
    FROM '@"COMPANY_TB_TEST"."TPCDS_SF10TCL"."LEGACY_PARQUET"'
)
PATTERN = '.*'
FILE_FORMAT = my_parquet_format2
ON_ERROR=ABORT_STATEMENT;
```

### 5. Transform Data to the Silver Layer Using Snowflake Notebook with Snowpark

Prepare the Silver layer by enriching and cleansing the data:
```python
# Import python packages
import streamlit as st
import pandas as pd

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, when, lit
# Snowpark for Python
from snowflake.snowpark.types import DoubleType
import snowflake.snowpark.functions as F

# We can also use Snowpark for our analyses!
from snowflake.snowpark.context import get_active_session
session = get_active_session()

-- Using Warehouse, Database, and Schema created during Setup
USE WAREHOUSE BILLION_RECORDS_WH;
USE DATABASE COMPANY_TB_TEST;
-- USE SCHEMA TPCDS_SF10TCL;
USE SCHEMA SILVER;

# Get Snowflake Session object
session = get_active_session()
session.sql_simplifier_enabled = True

# Drop table
drop table COMPANY_TB_TEST.SILVER.BILLION_RECORDS_ENRICHED_V2;


# Load the dataframes
billion_records_df = session.table("COMPANY_TB_TEST.BRONZE.BILLION_RECORDS_TABLE1")

customer_df = session.table("COMPANY_TB_TEST.TPCDS_SF10TCL.CUSTOMER")
date_dim_df = session.table("COMPANY_TB_TEST.TPCDS_SF10TCL.DATE_DIM")

# Perform the transformation with joins and calculated columns
transformed_df = (
    billion_records_df
    .join(customer_df, billion_records_df["SS_CUSTOMER_SK"] == customer_df["C_CUSTOMER_SK"], how="left")
    .join(date_dim_df, billion_records_df["SS_SOLD_DATE_SK"] == date_dim_df["D_DATE_SK"], how="left")
    .select(
        billion_records_df["SS_SOLD_DATE_SK"],
        billion_records_df["SS_SOLD_TIME_SK"],
        billion_records_df["SS_ITEM_SK"],
        billion_records_df["SS_CUSTOMER_SK"],
        billion_records_df["SS_CDEMO_SK"],
        billion_records_df["SS_HDEMO_SK"],
        billion_records_df["SS_ADDR_SK"],
        billion_records_df["SS_STORE_SK"],
        billion_records_df["SS_PROMO_SK"],
        billion_records_df["SS_TICKET_NUMBER"],
        billion_records_df["SS_QUANTITY"],
        billion_records_df["SS_WHOLESALE_COST"],
        billion_records_df["SS_LIST_PRICE"],
        billion_records_df["SS_SALES_PRICE"],
        billion_records_df["SS_EXT_DISCOUNT_AMT"],
        billion_records_df["SS_EXT_SALES_PRICE"],
        billion_records_df["SS_EXT_WHOLESALE_COST"],
        billion_records_df["SS_EXT_LIST_PRICE"],
        billion_records_df["SS_EXT_TAX"],
        billion_records_df["SS_COUPON_AMT"],
        billion_records_df["SS_NET_PAID"],
        billion_records_df["SS_NET_PAID_INC_TAX"],
        billion_records_df["SS_NET_PROFIT"],
        customer_df["C_CUSTOMER_ID"],
        customer_df["C_FIRST_NAME"],
        customer_df["C_LAST_NAME"],
        date_dim_df["D_DATE"],
        date_dim_df["D_DAY_NAME"],
        date_dim_df["D_QUARTER_NAME"],
        # Derived Metrics
        (billion_records_df["SS_SALES_PRICE"] - billion_records_df["SS_WHOLESALE_COST"]).alias("GROSS_PROFIT"),  # Gross profit calculation
        (billion_records_df["SS_EXT_DISCOUNT_AMT"] + billion_records_df["SS_COUPON_AMT"]).alias("TOTAL_DISCOUNT"),  # Total discount given
        (billion_records_df["SS_QUANTITY"] * billion_records_df["SS_SALES_PRICE"]).alias("TOTAL_REVENUE"),  # Total revenue for the transaction
        (billion_records_df["SS_EXT_SALES_PRICE"] / when(billion_records_df["SS_QUANTITY"] == 0, lit(1)).otherwise(billion_records_df["SS_QUANTITY"])).alias("AVERAGE_ITEM_PRICE"),  # Avoid division by zero
        # Categorizing profit levels
        when((billion_records_df["SS_SALES_PRICE"] - billion_records_df["SS_WHOLESALE_COST"]) > 500, lit('High Profit'))
        .when((billion_records_df["SS_SALES_PRICE"] - billion_records_df["SS_WHOLESALE_COST"]).between(100, 500), lit('Medium Profit'))
        .otherwise(lit('Low Profit')).alias("PROFIT_CATEGORY"),
        # Checking for significant discounts
        when(billion_records_df["SS_EXT_DISCOUNT_AMT"] > billion_records_df["SS_LIST_PRICE"] * 0.5, lit('High Discount'))
        .otherwise(lit('Regular Discount')).alias("DISCOUNT_CATEGORY")
    )
)

# Create the new table in Snowflake
transformed_df.write.mode("overwrite").save_as_table("COMPANY_TB_TEST.SILVER.BILLION_RECORDS_ENRICHED_V2")

select count(*) from BILLION_RECORDS_ENRICHED_V2;

```

![image](https://github.com/user-attachments/assets/d41163a9-ce84-4c62-8245-a3ee260e26c6)


![image](https://github.com/user-attachments/assets/b760bdd0-1e9c-438f-baed-9acfd3204bc2)



### 6. Aggregate Data for the Gold Layer
Define dynamic tables or views to produce insights from the Silver layer.

Example:
```sql
USE DATABASE COMPANY_TB_TEST;
USE SCHEMA GOLD;

    

-- 1. Total Revenue and Gross Profit by Quarter
CREATE OR REPLACE DYNAMIC TABLE COMPANY_TB_TEST.GOLD.QUARTERLY_SALES_SUMMARY
 target_lag = '20 minutes' refresh_mode = AUTO initialize = ON_CREATE warehouse = COMPUTE_WH
AS
SELECT 
    D_QUARTER_NAME,
    SUM(TOTAL_REVENUE) AS TOTAL_REVENUE,
    SUM(GROSS_PROFIT) AS TOTAL_GROSS_PROFIT,
    AVG(GROSS_PROFIT) AS AVG_GROSS_PROFIT
FROM 
    COMPANY_TB_TEST.SILVER.BILLION_RECORDS_ENRICHED
GROUP BY 
    D_QUARTER_NAME
ORDER BY 
    D_QUARTER_NAME;




-- 2. Profit Categories by Store
CREATE OR REPLACE DYNAMIC TABLE COMPANY_TB_TEST.GOLD.STORE_PROFIT_CATEGORY_SUMMARY
 target_lag = '20 minutes' refresh_mode = AUTO initialize = ON_CREATE warehouse = COMPUTE_WH
AS
SELECT 
    SS_STORE_SK,
    PROFIT_CATEGORY,
    COUNT(*) AS TRANSACTION_COUNT,
    SUM(TOTAL_REVENUE) AS TOTAL_REVENUE,
    SUM(GROSS_PROFIT) AS TOTAL_GROSS_PROFIT
FROM 
    COMPANY_TB_TEST.SILVER.BILLION_RECORDS_ENRICHED
GROUP BY 
    SS_STORE_SK, PROFIT_CATEGORY
ORDER BY 
    SS_STORE_SK, PROFIT_CATEGORY;



-- 3. Customer-Level Revenue and Discount Insights
CREATE OR REPLACE DYNAMIC TABLE COMPANY_TB_TEST.GOLD.CUSTOMER_SALES_SUMMARY
 target_lag = '20 minutes' refresh_mode = AUTO initialize = ON_CREATE warehouse = COMPUTE_WH
AS
SELECT 
    C_CUSTOMER_ID,
    C_FIRST_NAME,
    C_LAST_NAME,
    COUNT(*) AS TOTAL_TRANSACTIONS,
    SUM(TOTAL_REVENUE) AS TOTAL_REVENUE,
    SUM(TOTAL_DISCOUNT) AS TOTAL_DISCOUNT,
    AVG(TOTAL_REVENUE) AS AVG_REVENUE_PER_TRANSACTION,
    AVG(TOTAL_DISCOUNT) AS AVG_DISCOUNT_PER_TRANSACTION
FROM 
    COMPANY_TB_TEST.SILVER.BILLION_RECORDS_ENRICHED
GROUP BY 
    C_CUSTOMER_ID, C_FIRST_NAME, C_LAST_NAME
ORDER BY 
    TOTAL_REVENUE DESC;



-- 4. Daily Revenue and Profit Trend
CREATE OR REPLACE DYNAMIC TABLE COMPANY_TB_TEST.GOLD.DAILY_REVENUE_TRENDS
 target_lag = '20 minutes' refresh_mode = AUTO initialize = ON_CREATE warehouse = COMPUTE_WH
AS
SELECT 
    D_DATE,
    SUM(TOTAL_REVENUE) AS TOTAL_REVENUE,
    SUM(GROSS_PROFIT) AS TOTAL_GROSS_PROFIT,
    COUNT(*) AS TRANSACTION_COUNT
FROM 
    COMPANY_TB_TEST.SILVER.BILLION_RECORDS_ENRICHED
GROUP BY 
    D_DATE
ORDER BY 
    D_DATE;



-- 5. High Discount Transactions Analysis
CREATE OR REPLACE DYNAMIC TABLE COMPANY_TB_TEST.GOLD.HIGH_DISCOUNT_ANALYSIS
 target_lag = '20 minutes' refresh_mode = AUTO initialize = ON_CREATE warehouse = COMPUTE_WH
AS
SELECT 
    SS_STORE_SK,
    D_DATE,
    COUNT(*) AS HIGH_DISCOUNT_COUNT,
    SUM(TOTAL_REVENUE) AS TOTAL_REVENUE,
    SUM(GROSS_PROFIT) AS TOTAL_GROSS_PROFIT
FROM 
    COMPANY_TB_TEST.SILVER.BILLION_RECORDS_ENRICHED
WHERE 
    DISCOUNT_CATEGORY = 'High Discount'
GROUP BY 
    SS_STORE_SK, D_DATE
ORDER BY 
    HIGH_DISCOUNT_COUNT DESC;


```





---

## Performance Metrics

- **CSV Ingestion Duration:** 4 minutes 49 seconds (Warehouse Size: Large)
- **Parquet Ingestion Duration:** 2 minutes 45 seconds (Warehouse Size: Large)
- **Transformation Duration:** 6 minutes 14 seconds (Warehouse Size: Large)

---

## Contact
For questions or feedback, please reach out to [Your Name] at [Your Contact Information].

---

## License
This project is licensed under the [MIT License](LICENSE).


