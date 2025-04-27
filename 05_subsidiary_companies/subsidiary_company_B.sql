--IN THIS WORKSHEET WE WILL BE SETTING UP A READERS ACCOUNT FOR SUBSIDIARY_COMPANY_B SO THAT IT CAN ACCESS REQUISITE DATA ANDD ALSO HAVE A SEPARTE WORKSPACE FROM PRIMARY
--USE FAB_ADMIN ROLE

use fab_admin
use database fab_retailers;

--CREATING A SHARE
create or replace share subsidiary_company_B 
comment="data share for subsidiary company B";

--  granting usage on database and schema,select priviledge on view

grant usage on database fab_retailers to share subsidiary_company_B;
grant usage on schema FINANCE to share subsidiary_company_B;
grant usage on schema sales_marketing to share subsidiary_company_b;

grant usage on schema shared to share subsidiary_company_b;
grant select on view shared.store_performance_view to share subsidiary_company_b;

--creating reader account

create MANAGED ACCOUNT  SUBSIDIARY_B_USER
ADMIN_NAME='SUBSIDIARY_B_ADMIN'
ADMIN_PASSWORD='HeandIgotyou!123#'
TYPE=READER;
au82794.ap-southeast-1;


--to find out the account_location id

show managed accounts;

--to add account to the share

alter share subsidiary_company_b add accounts=au82794;
