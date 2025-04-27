--SO IN THIS WORKSHEET WE WOULD BE INJESTING THE DATA THAT IS THERE IS S3 AND CREATING THE HR RAW TABLE 

use role hr_role;
use database fab_retailers;
use schema hr;

--Lets look at the stages once

list @hr_employee_attendance;

list@hr_employee_data;

list @hr_performance;

--Lets create external tables for each stages
--first lets look at the structure
--hr_employee_attendance

select * from table(
infer_schema(
location=>'@hr_employee_attendance',
file_format=>'json_format'
)
);
drop table employee_attendance_ext;

--Creating the external table employee_attendance_raw

create or replace external table hr.employee_attendance_ext(
date_atten Date as (value:date::date),
employee_id string as (value:employee_id::string),
shift_time string as (value:shift_time::string),
status string as (value:status::string)
) 
with location=@hr_employee_attendance
file_format=(format_name='json_format');

select * from employee_attendance_ext;

--creating a snwoflake table using CTAS

create or replace table employee_attendance_raw
as 
select date_atten,employee_id,shift_time,status from employee_attendance_ext;

select * from employee_attendance_raw;

--Employee_data
--Sstructure of data

select * from table(
infer_schema(
location => '@hr_employee_data',
file_format => 'parquet_format'
));

--External table-employee_data_ext

create or replace external table hr.employee_data_ext (
employee_id string as (value:employee_id::string),
first_name string as (value:first_name::string),
last_name string as (value:last_name::string),
dob date as (value:dob::date),
email string as (value:email::string),
phone string as (value:phone::string),
hire_date date as (value:hire_date::date),
department string as (value:department::string),
salary int as (value:salary::INT)
)
with location =@hr_employee_data
file_format=parquet_format;

select * from employee_data_ext;
select $1 from @hr_employee_data;
//creating snowflake table
create or replace table employee_data_raw 
as
select employee_id,first_name,last_name,dob,email,phone,hire_date,department,salary from employee_data_ext;

--hr_performance stage

select * from table(
infer_schema(
location => '@hr_performance',
file_format => 'json_format'
));

create or replace external table hr.employee_performance_ext (
employee_id string as (value:employee_id::string),
evaluation_date string as (value:evaluation_date::string),
performance_score Int as (value:performance_score::int),
review_comments string as (value:review_comments::string)
)
with location =@hr_performance
file_format=json_format;

select * from employee_performance_ext;

--creating snowflake table

create or replace table employee_performance_raw as
select employee_id, evaluation_date, performance_score,review_comments from employee_performance_ext;

select * from employee_performance_raw;
