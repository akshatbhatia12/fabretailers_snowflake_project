--creating file format for hr depattments
--use role

use role hr_role;
use database fab_retailers;
use schema hr;

--creating file format
create or replace file format json_format
type='JSON'
strip_outer_array=True;

create or replace file format parquet_format 
type='parquet';

--Dept-IT
use role IT_ROLE;
use database fab_retailers;
use schema it;

create or replace file format json_format
type='json'
strip_outer_array=true;


create or replace file format csv_format
type='csv'
field_optionally_enclosed_by='"'
SKIP_HEADER=1
field_delimiter=','
NULL_IF=('\\N','NULL','');

--Dept-Finance
use role finance_role;
use database fab_retailers;
use schema finance;

create or replace file format csv_format
type='csv'
skip_header=1
null_if = ('', 'NULL')
field_delimiter=','
record_delimiter='\n'
;

--Dept-sales nd marketing
use role sales_marketing_role;
use database fab_retailers;
use schema sales_marketing;

CREATE OR REPLACE FILE FORMAT json_file_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;

create or replace file format csv_file_format
type='csv'
field_optionally_enclosed_by='"'
skip_header=1
field_delimiter=',';


//CREATING STORAGE INTEGRATION

use role accountadmin;

create or replace storage integration s3_fab_retail_integration
type=external_stage
storage_provider=S3
enabled=true
storage_aws_role_arn='arn:aws:iam::211346351751:role/fabretailers_storage_role2'
storage_allowed_locations=
('s3://snowflake-fabretailers-project/',
's3://snowflake-fabretailers-project/hr-department/',
's3://snowflake-fabretailers-project/hr-department/employee_attendance/',
's3://snowflake-fabretailers-project/hr-department/employee_data/',
's3://snowflake-fabretailers-project/hr-department/performance/',
's3://snowflake-fabretailers-project/it-department/',
's3://snowflake-fabretailers-project/it-department/live_stream_data/',
's3://snowflake-fabretailers-project/it-department/it_infrastructure/',
's3://snowflake-fabretailers-project/finance/',
's3://snowflake-fabretailers-project/finance/expenses/',
's3://snowflake-fabretailers-project/finance/revenue_summary/',
's3://snowflake-fabretailers-project/sales_marketing/',
's3://snowflake-fabretailers-project/sales_marketing/marketing_campaign/',
's3://snowflake-fabretailers-project/sales_marketing/store_locations/',
's3://snowflake-fabretailers-project/sales_marketing/store_sales/');

describe integration s3_fab_retail_integration;

grant usage on integration s3_fab_retail_integration to role hr_role;
grant usage on integration s3_fab_retail_integration to role it_role;
grant usage on integration s3_fab_retail_integration to role finance_role;
grant usage on integration s3_fab_retail_integration to role sales_marketing_role;

--CREATING EXTERNAL STAGES FOR EACH DATA PATH PRESENT IN S3 BUCKET
--Dept-hr-department

use role hr_role;
use database fab_retailers;
use schema hr;

create or replace stage hr_employee_attendance
url='s3://snowflake-fabretailers-project/hr-department/employee_attendance/'
storage_integration=s3_fab_retail_integration
file_format=json_format;

create or replace stage hr_employee_data
url='s3://snowflake-fabretailers-project/hr-department/employee_data/'
storage_integration=s3_fab_retail_integration
file_format=parquet_format;

create or replace stage hr_performance
url='s3://snowflake-fabretailers-project/hr-department/performance/'
storage_integration=s3_fab_retail_integration
file_format=json_format;

--Dept-finance-department

use role finance_role;
use database fab_retailers;
use schema finance;

create or replace stage finance_expenses
url='s3://snowflake-fabretailers-project/finance/expenses/'
storage_integration=s3_fab_retail_integration
file_format=csv_format;

create or replace stage finance_revenue_summary
url='s3://snowflake-fabretailers-project/finance/revenue_summary/revenue_summary.csv'
storage_integration=s3_fab_retail_integration
file_format=csv_format;

--Dept-sales_marketing
use role sales_marketing_role;
use database fab_retailers;
use schema sales_marketing;

create or replace stage sales_marketing_campaigns
url='s3://snowflake-fabretailers-project/sales-marketing/marketing_campaigns/marketing_campaigns.csv'
storage_integration=s3_fab_retail_integration
file_format=csv_file_format;

create or replace stage sales_marketing_store_locations
url='s3://snowflake-fabretailers-project/sales-marketing/store_locations/store_locations.json'
storage_integration=s3_fab_retail_integration
file_format=json_file_format;

create or replace stage sales_marketing_store_sales
url='s3://snowflake-fabretailers-project/sales-marketing/store-sales/'
storage_integration=s3_fab_retail_integration
file_format=csv_file_format;


--Dept-it_department

use role it_role;
use database fab_retailers;
use schema it;

create or replace stage it_infrastructure
url='s3://snowflake-fabretailers-project/it-department/it_infrastructure/it_infrastructure_data_part_1.csv'
storage_integration=s3_fab_retail_integration
file_format=csv_format;


create or replace stage it_live_data
url='s3://snowflake-fabretailers-project/it-department/live-stream-data/'
storage_integration=s3_fab_retail_integration
file_format=json_format;
