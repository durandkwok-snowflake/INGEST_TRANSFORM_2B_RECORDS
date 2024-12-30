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

### 5. Transform Data to the Silver Layer

Prepare the Silver layer by enriching and cleansing the data:
```sql
CREATE OR REPLACE TABLE COMPANY_TB_TEST.SILVER.BILLION_RECORDS_ENRICHED AS
SELECT
    SS_SOLD_DATE_SK,
    SS_SOLD_TIME_SK,
    SS_ITEM_SK,
    ... -- Additional Transformations
FROM
    COMPANY_TB_TEST.BRONZE.BILLION_RECORDS_TABLE1;
```

### 6. Aggregate Data for the Gold Layer
Define dynamic tables or views to produce insights from the Silver layer.

Example:
```sql
CREATE OR REPLACE DYNAMIC TABLE COMPANY_TB_TEST.GOLD.QUARTERLY_SALES_SUMMARY
WAREHOUSE = 'LARGE'
AS
SELECT
    D_QUARTER_NAME,
    SUM(TOTAL_REVENUE) AS TOTAL_REVENUE,
    SUM(GROSS_PROFIT) AS TOTAL_GROSS_PROFIT
FROM
    COMPANY_TB_TEST.SILVER.BILLION_RECORDS_ENRICHED
GROUP BY
    D_QUARTER_NAME;
```

---

## Performance Metrics

- **CSV Ingestion Duration:** 4 minutes 49 seconds (Warehouse Size: Large)
- **Parquet Ingestion Duration:** 2 minutes 45 seconds (Warehouse Size: Large)

---

## Contact
For questions or feedback, please reach out to [Your Name] at [Your Contact Information].

---

## License
This project is licensed under the [MIT License](LICENSE).


