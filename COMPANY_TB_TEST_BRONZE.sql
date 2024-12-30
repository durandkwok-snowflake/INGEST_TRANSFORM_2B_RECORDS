
-- Create database and schema
CREATE OR REPLACE DATABASE COMPANY_TB_TEST;
USE DATABASE COMPANY_TB_TEST;
CREATE SCHEMA BRONZE;
use SCHEMA BRONZE;

-- Create table
create or replace TABLE billion_records_table1 (
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



---------------------------------------------------------------------------------------------
-- !!!!!!!!!!!!!!!!!!!!!!!
-- Begin 

-- 2 Billion Records Load Test
truncate table billion_records_table1;
select * from billion_records_table1 limit 10;


-- Create stage for CSV files
CREATE STAGE TB_STAGE URL = 's3://mybucket/mystage' CREDENTIALS = (
  aws_key_id = 'my_aws_key_id' aws_secret_key = 'my_aws_secret_key'
);

-- Create stage for PARQUET files
CREATE STAGE LEGACY_PARQUET URL = 's3://mybucket/mystage' CREDENTIALS = (
  aws_key_id = 'my_aws_key_id' aws_secret_key = 'my_aws_secret_key'
);



-- Option 1 with CSV files

LIST @COMPANY_TB_TEST.TPCDS_SF10TCL.TB_STAGE;


-- Query Id 01b939a4-0004-61ef-0015-93870ad2c476
-- Query Duration - 4m 49s
-- Start Time 12/23/2024, 10:32:45 AM
-- End Time 12/23/2024, 10:37:34 AM
-- Warehouse Size Large
-- --2,236,190,720

COPY INTO "COMPANY_TB_TEST"."BRONZE"."BILLION_RECORDS_TABLE1" 
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23
    FROM '@"COMPANY_TB_TEST"."TPCDS_SF10TCL"."TB_STAGE"'
)
FORCE=TRUE
--PATTERN='.*data_0_[01]_.*\.csv\.gz'
-- PATTERN='.*data_0_[0-2]_.*\.csv\.gz'
PATTERN='.*data_0_[0-4]_.*\.csv\.gz'
ON_ERROR=ABORT_STATEMENT;


select count(*) from billion_records_table1;
select * from billion_records_table1 limit 10;



-- Option 2 - Parquet format
truncate table billion_records_table1;
select * from billion_records_table1 limit 10;

CREATE OR REPLACE FILE FORMAT my_parquet_format2
  TYPE = PARQUET
  USE_VECTORIZED_SCANNER = TRUE,
    REPLACE_INVALID_CHARACTERS=TRUE,
    BINARY_AS_TEXT=FALSE
    ;

-- Query Id 01b95123-0004-671d-0015-93870b67dc9e
-- Query Duration - 2m 45s
-- Start Time 12/27/2024, 2:47:31 PM
-- End Time 12/27/2024, 2:50:16 PM
-- Warehouse Size Large
-- --2,236,190,720

COPY INTO "COMPANY_TB_TEST"."BRONZE"."BILLION_RECORDS_TABLE1"
FROM (
    SELECT $1:_COL_0::NUMBER(38, 0), $1:_COL_1::NUMBER(38, 0), $1:_COL_2::NUMBER(38, 0), $1:_COL_3::NUMBER(38, 0), $1:_COL_4::NUMBER(38, 0), $1:_COL_5::NUMBER(38, 0), $1:_COL_6::NUMBER(38, 0), $1:_COL_7::NUMBER(38, 0), $1:_COL_8::NUMBER(38, 0), $1:_COL_9::NUMBER(38, 0), $1:_COL_10::NUMBER(38, 0), $1:_COL_11::NUMBER(7, 2), $1:_COL_12::NUMBER(7, 2), $1:_COL_13::NUMBER(7, 2), $1:_COL_14::NUMBER(7, 2), $1:_COL_15::NUMBER(7, 2), $1:_COL_16::NUMBER(7, 2), $1:_COL_17::NUMBER(7, 2), $1:_COL_18::NUMBER(7, 2), $1:_COL_19::NUMBER(7, 2), $1:_COL_20::NUMBER(7, 2), $1:_COL_21::NUMBER(7, 2), $1:_COL_22::NUMBER(7, 2)
    FROM '@"COMPANY_TB_TEST"."TPCDS_SF10TCL"."LEGACY_PARQUET"'
)
PATTERN = '.*'
FILE_FORMAT = my_parquet_format2
ON_ERROR=ABORT_STATEMENT;


