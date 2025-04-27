--SO IN THIS WORKSHEET WE WOULD BE INJESTING THE DATA THAT IS THERE IS S3 AND CREATING THE FINANCE RAW TABLE 

use role finance_role;
use database fab_retailers;
use schema finance;

--list the stages first

list @finance_expenses;
list @finance_revenue_summary;

--checking the structure of the data
//finance expense stage

select * from table(
infer_schema(
location => '@finance_expenses',
file_format => 'csv_format'
));

select $1 as expense_id,$2 as employee_id,$3 as category,$4 as amount,$5 as dates,$6 as comments from  @finance_expenses;
drop table expenses_ext;

--since it is a structured format
--directly creating internal snowflake table

create or replace table finance_expenses_raw(
expense_id String,
employee_id string,
category string,
amount int,
dates date,
comments string
);

insert into finance_expenses_raw 
(select $1,$2,$3,$4,$5,$6 from @finance_expenses);

select * from finance_expenses_raw;

--finance revenue summary
select * from table(
infer_schema(
location => '@finance_revenue_summary',
file_format => 'csv_format'
));
SELECT $2 FROM @FINANCE_REVENUE_SUMMARY;

--since it is a structured format
--directly creating internal snowflake table 

CREATE OR REPLACE TABLE REVENUE_RAW (
REVENUE_ID STRING,
STORE_ID STRING,
REVENUE_AMOUNT STRING,
REVENUE_DATE DATE,
REGION STRING
);

INSERT INTO REVENUE_RAW(
SELECT $1,$2,$3,$4,$5 from  @finance_revenue_summary
);

--PLEASE NOTE- SINCE BOTH THE TABLES WERE .CSV FORMAT (STRUCTURED) WE SKIPPED CREATING EXTERNAL TABLE AND DIRECTLY CREATED SNOWFLAKE-MANAGED TABLE
