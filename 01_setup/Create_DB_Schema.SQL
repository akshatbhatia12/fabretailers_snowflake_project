--creating the database-"FAB_RETAILERS" whcih will have all the department schemas

create or replace database FAB_RETAILERS;

--use database

Use database Fab_retailers;

--Creating department wise schemas

create or replace schema HR;
CREATE OR REPLACE SCHEMA IT;
CREATE OR REPLACE SCHEMA FINANCE;
CREATE OR REPLACE SCHEMA SALES_MARKETING;

--CREATING A SHARED SCHEMA
CREATE OR REPLACE SCHEMA SHARED;
