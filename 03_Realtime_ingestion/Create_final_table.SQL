--IN THIS WORKSHEET, ALL THE BASIC STRUCTURE OF FINAL TABLE IS CREATED

--IT department

use role it_role;
use database fab_retailers;
use schema it;

--it_live_data
CREATE OR REPLACE TABLE it_live_table_processed (
  device_id STRING,
  event_details STRING,
  event_id INT,
  event_timestamp TIMESTAMP_NTZ,
  event_type STRING,
  ingestion_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);
//one-time manual transfer of data
INSERT INTO it_live_table_processed (
  device_id,
  event_details,
  event_id,
  event_timestamp,
  event_type
)
SELECT
  device_id,
  event_details,
  event_id,
  event_timestamp,
  event_type
FROM it_live_table_raw_pipe;

--As the infrastruture final file is a static file so we have created the final file from CTAS

create or replace table it_infrastructure_final
as select * from it_infrastructure_raw;
select * from it_infrastructure_final;

--HR DEPARTMENT

use role hr_role;
use database fab_retailers;
use schema hr;
use warehouse hr_warehouse;

--employee_final_performance

CREATE OR REPLACE TABLE employee_performance_final (
    employee_id STRING,
    evaluation_date DATE,
    performance_score INT,
    review_comments STRING
);

--employee_attendance_final
create or replace table employee_attendance_final(
date_atten Date,
employee_id string,
shift_time string,
status string
);

--employee_table

create or replace table employee_data_final(
    employee_id string,
    first_name STRING,
    last_name STRING,
    dob date,
    email string,
    phone string,
    phone_extension string,
    hire_date string,
    department string);

-- SALES-MARKETING DEPARTMENT

use role sales_marketing_role;
use database fab_retailers;
use schema sales_marketing ;
use warehouse sales_marketing_warehouse;

--MARKETING_CAMPAIGN_FINAL

create or replace table marketing_campaign_final (
campaign_id string,
campaign_name string,
start_date date,
end_date date,
budget int,
status string,
store_id string);

--store_locations_final

create or replace table store_locations_final(
city string,
contact_info string,
extension string,
region  string,
store_id string,
store_manager string,
store_name string
);

-- store_sales_final

create or replace table store_sales_final(
sales_id string,
store_id string,
product string,
quantity_sold int,
sale_price int,
total_sale int,
payment_method string,
sale_date date 
);

--finance department

create or replace table revenue_final (
revenue_id string,
    store_id string,
    revenue_amount string,
    revenue_date string,
    region string);


    create or replace table finance_expenses_final (
expense_id string,
    employee_id string,
    expense_type string,
    amount INT,
    expense_date date,
    expense_description string

    
    );
