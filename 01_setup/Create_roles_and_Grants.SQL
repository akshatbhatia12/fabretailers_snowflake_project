--Create top-level role

Create or replace role FAB_ADMIN;

--Creating department specific roles

create or replace role HR_ROLE;
CREATE OR REPLACE ROLE IT_ROLE;
CREATE OR REPLACE ROLE FINANCE_ROLE;
CREATE OR REPLACE ROLE SALES_MARKETING_ROLE;

--CREATE SHARED ACCESS ROLE(if needed by multiple departments)

CREATE OR REPLACE ROLE SHARED_ACCESS_ROLE;

--GRANT USAGE of DATABASE to ALL ROLES roles

GRANT USAGE ON DATABASE FAB_RETAILERS TO ROLE FAB_ADMIN;
GRANT USAGE ON DATABASE FAB_RETAILERS TO ROLE HR_ROLE;
GRANT USAGE ON DATABASE FAB_RETAILERS TO ROLE IT_ROLE;
GRANT USAGE ON DATABASE FAB_RETAILERS TO ROLE FINANCE_ROLE;
GRANT USAGE ON DATABASE FAB_RETAILERS TO ROLE SALES_MARKETING_ROLE;
GRANT USAGE ON DATABASE FAB_RETAILERS TO ROLE SHARED_ACCESS_ROLE;

--GRANT OWNERSHIP TO APPROPRIATE ROLE

GRANT OWNERSHIP ON SCHEMA FAB_RETAILERS.FINANCE TO ROLE FINANCE_ROLE REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA FAB_RETAILERS.HR TO ROLE HR_ROLE REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA FAB_RETAILERS.IT TO ROLE IT_ROLE REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA FAB_RETAILERS.SALES_MARKETING TO ROLE SALES_MARKETING_ROLE REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA FAB_RETAILERS.SHARED TO ROLE SHARED_ACCESS_ROLE REVOKE CURRENT GRANTS;

--Making sure that FAB_ADMIN ACTS AS A TOP LEVEL ROLE

grant all privileges on database fab_retailers to role fab_admin;

--Granting all roles to fab admin

grant role hr_role to role fab_admin;
grant role it_role to role fab_admin;
grant role sales_marketing_role to role fab_admin;
grant role finance_role to role fab_admin;
grant role shared_access_role to role fab_admin;

--Assigning the fab-admin role to user fabadmin

grant role fab_admin to user fab_admin;
grant role securityadmin to role fab_admin;

--Granting usage on warehouse

grant role accountadmin to role fab_admin;
GRANT EXECUTE ON  task FAB_RETAILERS.IT.TASK_INSERT_INTO_PROCESSED TO ROLE it_role;

--Account admin granting schema it,hr,finance,sales_marketing the execute task priviledge

use accountadmin

grant execute task on account to role it_role;
grant execute task on account to role finance_role;
grant execute task on account to role hr_role;
grant execute task on account to role sales_marketing_role;
