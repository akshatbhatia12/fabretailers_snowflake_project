--IN THIS WORKSHEET WE WILL BE CREATING IT RAW TABLES
use role it_role;
use database fab_retailers;
use schema it;
use warehouse it_warehouse;

--list the stages first
list @FAB_RETAILERS.IT.IT_INFRASTRUCTURE;
list @FAB_RETAILERS.IT.IT_LIVE_DATA;

--checking the structure of the data
--IT Infrastructure stage
select * from table(
infer_schema(
location => '@it_infrastructure',
file_format => 'csv_format'
));
select $1 as device_id,$2 as department,$3 as device_type,$4 as operating_system,$5 as device_status,$6 as purchase_date,$7 as last_service_date,$8 as location from  @it_infrastructure;

--since it is a structured format
--directly creating internal snowflake table

create or replace table it_infrastructure_raw(
device_id String,
department string,
device_type string,
operating_system string,
device_status string,
purchase_date date,
last_service_date date,
location string
);
insert into it_infrastructure_raw 
(select $1,$2,$3,$4,$5,$6,$7,$8 from @it_infrastructure);

SELECT * FROM IT_INFRASTRUCTURE_RAW;

--IT_live_data
--checking the structure of the stage

select * from table(
infer_schema(
location => '@it_live_data',
file_format => 'json_format'
));

--Option-1
--creating the external table _ext with auto ingest as true

create or replace external table it_live_data_ext(
device_id string as (value:device_id::string),
event_details string as (value:event_details::string),
event_id int as (value:event_id::int),
event_timestamp timestamp_ntz as (value:event_timestamp::timestamp_ntz),
event_type string as (value:event_type::string)
) 
with location=@it_live_data
file_format=(format_name='json_format')
auto_refresh=true;

select * from it_live_data_ext;

--creating a snowflake table using CTAS
create or replace table it_live_table_raw
as
select device_id,event_details,event_id,event_timestamp,event_type from it_live_data_ext;

select * from it_live_table_raw;

--Option-2 
--creating internal table for auto ingestion

CREATE OR REPLACE TABLE it_live_table_raw_pipe (
  device_id STRING,
  event_details STRING,
  event_id INT,
  event_timestamp TIMESTAMP_NTZ,
  event_type STRING
);
--as there were some files already there in external stage

create or replace external table it_live_data_ext1(
device_id string as (value:device_id::string),
event_details string as (value:event_details::string),
event_id int as (value:event_id::int),
event_timestamp timestamp_ntz as (value:event_timestamp::timestamp_ntz),
event_type string as (value:event_type::string)
) 
with location=@it_live_data
file_format=(format_name='json_format');

--inserting those values in the internal table on which pipe is configured
insert into it_live_table_raw_pipe(
select device_id,event_details,event_id,event_timestamp,event_type from it_live_data_ext
);
