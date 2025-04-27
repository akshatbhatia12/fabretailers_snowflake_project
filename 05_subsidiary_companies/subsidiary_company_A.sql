--IN THIS WORKSHEET WE WILL BE SETTING UP A READERS ACCOUNT FOR SUBSIDIARY_COMPANY_A SO THAT IT CAN ACCESS REQUISITE DATA ANDD ALSO HAVE A SEPARTE WORKSPACE FROM PRIMARY
--USE FAB_ADMIN ROLE
use fab_admin
use database fab_retailers;

--CREATING A SHARE
create or replace share subsidiary_company_A
comment="data share for subsidiary company A";

--  granting usage on database and schema,select priviledge on view

grant usage on database fab_retailers to share subsidiary_company_A;
grant usage on schema shared to share subsidiary_company_A;
GRANT SELECT ON VIEW SHARED.SALES_MARKETING_STORE_JOINED TO SHARE SUBSIDIARY_COMPANY_A;
SHOW GRANTS TO SHARE subsidiary_company_a;

--creating reader account

create MANAGED ACCOUNT  SUBSIDIARY_A_USER
ADMIN_NAME='SUBSIDIARY_A_ADMIN'
ADMIN_PASSWORD='HeandIgotyou!123#'
TYPE=READER;

--to find out the account_location id
show managed accounts;

--to add account to the share
alter share subsidiary_company_A add accounts =MW14406;
