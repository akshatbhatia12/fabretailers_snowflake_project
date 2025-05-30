--creating dynamic data masking policies for each department which require to hide sensitive data

--hr_department
--will use that schema for hr
--accountadmin role/fab_admin role to be used to create these
use schema hr;

--EMAIL MASKING POLICY

create or replace masking policy email_masking_policy as (val string) 
returns string->
    case 
        when current_role() in ('HR_ROLE','FAB_ADMIN','ACCOUNTADMIN') then val
        else concat('XXX@',SPLIT_PART(VAL,'@',2))
    END;

--PHONE NUMBER MASKING POLICY

CREATE OR REPLACE MASKING POLICY PHONE_MASK_POLICY AS(VAL STRING)
RETURNS STRING->
    CASE
        WHEN CURRENT_ROLE() IN ('HR_ROLE','FAB_ADMIN','ACCOUNTADMIN') THEN VAL
        ELSE 'XXX-XXX-' || RIGHT(val,4)
    end;

--SETTING MASKING POLICY ON RESPECTIVE TABLES

ALTER TABLE EMPLOYEE_DATA_FINAL
    MODIFY COLUMN EMAIL SET MASKING POLICY EMAIL_MASKING_POLICY;

ALTER TABLE EMPLOYEE_DATA_FINAL
    MODIFY COLUMN PHONE  SET MASKING POLICY PHONE_MASK_POLICY;

--FINANCE DEPATMENT
--USE FINANCE SCHEMA

USE SCHEMA FINANCE
--NO DATA HAS TO BE MASKED IN FINANCE DEPT AS IT LACKS SENSITIVE DATA 

--SALES_MARKETING DEPARTMENT
--USE SALES_MARKETING SCHEMA
USE SCHEMA SALES_MARKETING_SCHEMA

--TO CHECK SENSITIVE DATA   
SELECT * FROM MARKETING_CAMPAIGN_FINAL;
SELECT * FROM STORE_LOCATIONS_FINAL;
SELECT * FROM STORE_SALES_FINAL;--(DOESNT REQUIRE MASKING POLICY)

--BUDGET MASKING POLICY

CREATE OR REPLACE MASKING POLICY BUDGET_MASKING AS (VAL INT)
RETURNS INT->
    CASE
        WHEN CURRENT_ROLE() IN ('SALES_MARKETING_ROLE','FAB_ADMIN','ACCOUNTADMIN') THEN VAL
        ELSE NULL
    END;

--PHONE NUMBER MASKING POLICY
CREATE OR REPLACE MASKING POLICY PHONE_MASK_POLICY AS(VAL STRING)
RETURNS STRING->
    CASE
        WHEN CURRENT_ROLE() IN ('HR_ROLE','FAB_ADMIN','ACCOUNTADMIN') THEN VAL
        ELSE 'XXX-XXX-' || RIGHT(val,4)
    end;
    
    
    
--SETTING MASKING POLICY ON MARKETING_CAMPAIGN_FINAL

ALTER TABLE MARKETING_CAMPAIGN_FINAL 
MODIFY COLUMN BUDGET SET MASKING POLICY BUDGET_MASKING;

--SETING PHONE MASKING POLICY ON STORE_LOCATION_FINAL
ALTER TABLE STORE_LOCATIONS_FINAL 
MODIFY COLUMN CONTACT_INFO SET MASKING POLICY PHONE_MASK_POLICY;

--IT_department
--will use that schema for hr
use schema IT;

SELECT * FROM IT_INFRASTRUCTURE_FINAL;
SELECT * FROM IT_LIVE_TABLE_PROCESSED;
--NO DATA HAS TO BE MASKED IN IT DEPT AS IT LACKS SENSITIVE DATA 
