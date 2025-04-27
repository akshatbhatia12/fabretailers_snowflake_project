--SO IN THIS WORKSHEET WE WOULD BE INJESTING THE DATA THAT IS THERE IS S3 AND CREATING THE SALES AND MARKETING RAW TABLE 

use role sales_marketing_role;
use database fab_retailers;
use schema sales_marketing;
use warehouse sales_marketing_warehouse;

--listing both the stages

list @SALES_MARKETING.SALES_MARKETING_CAMPAIGNS;
list @SALES_MARKETING.SALES_MARKETING_STORE_LOCATIONS;
list @SALES_MARKETING.SALES_MARKETING_STORE_SALES;

--Sales marketing campaign stage
--Checking the structure of the data

select * from table(
infer_schema(
location => '@sales_marketing_campaigns',
file_format => 'csv_file_format'
));

select $1 as campaign_id,$2 as acmpaign_name ,$3 as start_date,$4 as end_date,$5 as budget,$6 as status,$7 as store_id from @sales_marketing_campaigns;

--Since it is a structured format
--Directly creating internal snowflake table

create or replace table marketing_campaign_raw (
campaign_id string,
campaign_name  string,
start_date  date,
end_date date,
budget  int,
status  string,
store_id string
);
insert into marketing_campaign_raw 
(select $1,$2,$3,$4,$5,$6,$7 from @sales_marketing_campaigns);

select * from marketing_campaign_raw;

--sales marketing store sales
--checking the structure of the stage

select * from table(
infer_schema(
location => '@sales_marketing_store_sales',
file_format => 'csv_file_format'
));
select $1 as sales_id,$2 as store_id ,$3 as product,$4 as quantity_sold,$5 as sale_price,$6 as total_sale,$7 as payment_method,$8 as sale_date from @sales_marketing_store_sales;

--since it is a structured format
--directly creating internal snowflake table 

create or replace table store_sales_raw(
sales_id string,
store_id string,
product string,
quantity_sold INT,
sale_price float,
total_sale float,
payment_method string,
sale_date date
);

insert into store_sales_raw (
select $1,$2,$3,$4,$5,$6,$7,$8 from @sales_marketing_store_sales
);
select * from store_sales_raw;


--sales marketing store location
--checking the structure of the stage

select * from table(
infer_schema(
location => '@sales_marketing_store_locations',
file_format => 'json_file_format'
));
select $1:city,$1: from @sales_marketing_store_locations;

--Taking a shorter route and directly creating Snowflake managed table

CREATE OR REPLACE TABLE store_location_table (
    city STRING,
    contact_info STRING,
    region STRING,
    store_id STRING,
    store_manager STRING,
    store_name STRING
);
COPY INTO store_location_table
FROM (
    SELECT
        $1:city::STRING,
        $1:contact_info::STRING,
        $1:region::STRING,
        $1:store_id::STRING,
        $1:store_manager::STRING,
        $1:store_name::STRING
    FROM @sales_marketing_store_locations
)
FILE_FORMAT = (format_name = 'json_file_format');

--Creating a snowflake table using CTAS

create or replace table store_locations_raw 
as
select city,contact_info,region,store_id,store_manager,store_name from store_location_table;
select * from store_locations_raw;


insert into table store_locations_raw (

)
