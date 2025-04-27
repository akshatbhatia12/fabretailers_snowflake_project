--THIS ILE CONTAINS ALL THE SQL CODES USED TO CREATE FAB_RETAILERS_DASHBOARD WITH SOME EXPLANATION, THE .PNG OF DASHBOARD WILL BE UPLOADED SEPARTELY

--TILE 1- PRODUCT SOLD PER STORE
--TO KNOW WHICH PRODUCT IS MOST SOLD IN EACH DEPARTMENT, AND ALSO TO CAPARE IT WITH OTHER DEPARTMENTS

select store_id,product,sum(quantity_sold) from sales_marketing.sales_marketing_store_joined
group by store_id,product
order by store_id
;

--TILE 2- STORE INFORMATION
--TO KNOW ABOUT THE STORE DETAILS AND PERSONEL MANAGING IT

select store_id,store_manager,store_name from shared.store_performance_view;

--TILE 3-TOTAL REVENUE AND TOTAL SALES PER STORE
--AS THE TITLE SUGGEEST

select total_revenue,total_sale,store_id from shared.store_performance_view;

--TILE 4-EMPLOYEE STATUS
--THIS DEMARCATES BETWEEN PERSONELS PRESENT,ABSENT OR ON LEAVE FOR EACH DATE BRACKET SELECTED,KEEPS ON CHANGING THROUGH FILTER TOGGLE

select 
date_atten,
count(*),
sum(case when status='Present' then 1 else 0 end) as Present,
sum(case when status='Absent' then 1 else 0 end) as Absent,
sum (case when status='On Leave' then 1 else 0 end) as On_Leave,
from hr.employee_attendance_final
where date_atten=:daterange
group by date_atten;

--TILE 5-TOTAL EMPLOYEE
--COUNTING THE TOTAL NUMBER OF PERSONELS THERE IN THE COMPANY AT CURRENT 

SELECT COUNT(*) FROM HR.EMPLOYEE_DATA_FINAL;

--TILE 6-DEPARTMENT WISE EMPLOYEE
--DEPARTMENT WISE DEMARCATION OF THE PERSONEL

select total_revenue,total_sale,store_id from shared.store_performance_view;

--TILE 7-DEVICE STATUS
--ON TOP THE LIVE DATA FEED OF DEVICE STATUS, HELPS US DEMARCATE BETWEEN ERROR,SHUTDOWN,DEVICE CHECK,MAINTAINANCE,LOGIN STATUSES AND SUM EACH CATEGORY FOR AN OVERALL VEIW

SELECT
COUNT(DEVICE_ID),
  SUM(CASE WHEN event_type = 'Login' THEN 1 ELSE 0 END) AS Login,
  SUM(CASE WHEN event_type = 'Error' THEN 1 ELSE 0 END) AS Error,
  SUM(CASE WHEN event_type = 'Shutdown' THEN 1 ELSE 0 END) AS Shutdown,
  SUM(CASE WHEN event_type = 'Device Check' THEN 1 ELSE 0 END) AS Device_Check,
  SUM(CASE WHEN event_type = 'Maintenance' THEN 1 ELSE 0 END) AS Maintenance
FROM it.it_live_table_processed;

--PLEASE NOTE- THE ORDER OF THE TILES IS STRUCTURED TO KEEP ALL THE DEPARTMENT TILE TOGETHER
