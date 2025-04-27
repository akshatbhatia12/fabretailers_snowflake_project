--ONE OF THE *MOST IMPORTANT* FILE FOR AUTO INGESTION
--IN THIS WORKSHEET, WE CREATE STREAM(ON TOP OF RAW)->PROCEDURE->TASK
--THIS IS DONE, SO THAT IF THE RAW TABLE IS INSERTED WITH NEW DATA,UPDATED OR DELETED, IT WILL AUTOMATICALLY REFLECT IN FINAL TABLE OF THE RESPECTIVE DEPARTMENT

--IT DEPARTMENT
--TASK-1=> Create pipe for it_live_table_raw

use role it_role;
use database fab_retailers;
use schema it;
use warehouse it_warehouse;

--pipe_ddl
--this is if option 1 chosen to create the final table
--table->it_live_processed
CREATE OR REPLACE PIPE it_live_pipe
auto_ingest=true
AS
COPY INTO it_live_table_raw_pipe(
  device_id,
  event_details,
  event_id,
  event_timestamp,
  event_type
)
FROM (
  SELECT 
    $1:device_id::STRING,
    $1:event_details::STRING,
    $1:event_id::INT,
    $1:event_timestamp::TIMESTAMP_NTZ,
    $1:event_type::STRING
  FROM @it_live_data (FILE_FORMAT => 'json_format')
)
ON_ERROR = 'CONTINUE';

DESCRIBE PIPE IT_LIVE_PIPE;


--CREATING STREAMS AND TASKS
--Stream
--this is if option two is selected

create or replace stream it_live_stream 
on table it_live_table_raw_pipe
append_only=true;

--tasks

create or replace task fab_retailers.it.task_insert_into_processed
warehouse=it_warehouse
schedule='using cron */5 * * * * UTC'
when system$stream_has_data('it_live_stream')
as
insert into it_live_table_processed(
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
FROM it_live_stream;
);
alter task task_insert_into_processed Resume;

select * from it_live_table_processed;

--FINANCE DEPARTMENT

use role finance_role;
use database fab_retailers;
use schema finance;
use warehouse finance_warehouse;

--TASK-2--USING STREAM TASK TO CLEAN THE DATA 
-- Create a stream for finance_expenses_raw table
--table->finance_expenses_final

CREATE OR REPLACE STREAM finance_expenses_stream
ON TABLE finance_expenses_raw
APPEND_ONLY = FALSE;
-- Create a task to sync data into finance_expenses_final table

CREATE OR REPLACE TASK task_sync_expenses_to_final
  WAREHOUSE = finance_warehouse
  SCHEDULE = 'USING CRON */5 * * * * UTC'  -- runs every 5 minutes
  WHEN SYSTEM$STREAM_HAS_DATA('finance_expenses_stream')
AS
  MERGE INTO finance_expenses_final AS target
  USING (
    SELECT
      expense_id,
      employee_id,
      CATEGORY AS EXPENSE_TYPE,
      CASE WHEN amount > 5000 THEN 5000 ELSE amount END AS amount,  -- Cap extreme values for expenses
      DATES AS expense_date,
      COMMENTS AS expense_description
    FROM finance_expenses_stream
    WHERE expense_id IS NOT NULL  -- Exclude records with null expense_id
    AND amount IS NOT NULL  -- Exclude records with null amount
    GROUP BY expense_id  -- Remove duplicates based on expense_id
  ) AS source
  ON target.expense_id = source.expense_id
  WHEN MATCHED AND source.metadata$action = 'DELETE' THEN
    DELETE
  WHEN MATCHED AND source.metadata$action = 'UPDATE' THEN
    UPDATE SET
      target.expense_id = source.expense_id,
      target.employee_id = source.employee_id,
      target.expense_type = source.CATEGORY,
      target.amount = source.amount,
      target.expense_date = source.dates,
      target.expense_description = source.COMMENT
  WHEN NOT MATCHED THEN
    INSERT (
      expense_id, employee_id, expense_type, amount, expense_date, expense_description
    )
    VALUES (
      source.expense_id, source.employee_id, source.CATEGORY, source.amount, source.dates, source.comments
    );

alter task task_sync_expenses_to_final resume;

--one time upload 

INSERT INTO finance_expenses_final (
    expense_id,
    employee_id,
    expense_type,
    amount,
    expense_date,
    expense_description
)
SELECT
    expense_id,
    employee_id,
    category,
    CASE WHEN amount > 5000 THEN 5000 ELSE amount END AS amount,  -- Cap extreme values for expenses
    dates as expense_date,
    comments as expense_description 
FROM finance_expenses_raw
WHERE expense_id IS NOT NULL  -- Exclude records with null expense_id
  AND amount IS NOT NULL  -- Exclude records with null amount
  AND expense_date IS NOT NULL  -- Exclude records with null expense_date
  AND expense_description IS NOT NULL;  -- Exclude records with null expense_description

--table=>finance revenue_final 
--stream

create or replace stream revenue_raw_stream 
on table revenue_raw
append_only=False;
--task

CREATE OR REPLACE TASK task_merge_into_revenue_final
WAREHOUSE = finance_warehouse  -- (your Snowflake virtual warehouse)
SCHEDULE = 'USING CRON */5 * * * * UTC'  -- runs every 5 minutes
WHEN SYSTEM$STREAM_HAS_DATA('revenue_raw_stream')
AS
MERGE INTO revenue_final AS target
USING (
    SELECT 
        revenue_id,
        store_id,
        TRY_CAST(revenue_amount AS FLOAT) AS revenue_amount,
        revenue_date,
        region,
        METADATA$ACTION,
        METADATA$ISUPDATE
    FROM revenue_raw_stream
) AS source
ON target.revenue_id = source.revenue_id
-- If matching record exists and action is UPDATE, then UPDATE it
WHEN MATCHED AND source.METADATA$ACTION = 'UPDATE' THEN
    UPDATE SET
        target.store_id = source.store_id,
        target.revenue_amount = CASE 
                                    WHEN source.revenue_amount > 10000 THEN 10000 
                                    WHEN source.revenue_amount IS NULL THEN 0
                                    ELSE source.revenue_amount
                                END,
        target.revenue_date = source.revenue_date,
        target.region = source.region
-- If matching record exists and action is DELETE, then DELETE it
WHEN MATCHED AND source.METADATA$ACTION = 'DELETE' THEN
    DELETE
-- If no matching record and action is INSERT, then INSERT it
WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT' THEN
    INSERT (revenue_id, store_id, revenue_amount, revenue_date, region)
    VALUES (
        source.revenue_id,
        source.store_id,
        CASE 
            WHEN source.revenue_amount > 10000 THEN 10000 
            WHEN source.revenue_amount IS NULL THEN 0
            ELSE source.revenue_amount
        END,
        source.revenue_date,
        source.region
    );

alter task task_merge_into_revenue_final resume;

--one-time upload
-- Insert only the current data from revenue_raw into revenue_final
INSERT INTO revenue_final (
    revenue_id,
    store_id,
    revenue_amount,
    revenue_date,
    region
)
SELECT
    revenue_id,
    store_id,
    -- Clean anomalies: convert to FLOAT, cap extreme values, handle NULLs
    CASE 
        WHEN TRY_CAST(revenue_amount AS FLOAT) > 10000 THEN 10000 
        WHEN TRY_CAST(revenue_amount AS FLOAT) IS NULL THEN 0 
        ELSE TRY_CAST(revenue_amount AS FLOAT)
    END AS revenue_amount,
    revenue_date,
    region
FROM revenue_raw;
select * from revenue_final where store_id='ST001';


--HR DEPARTMENT

use role hr_role;
use database fab_retailers;
use schema hr;
use warehouse hr_warehouse;

--TASK 3-> CLEANING THE DATA IN HR SCHEMA 
--table-> hr_performance_final
--stream

CREATE OR REPLACE STREAM employee_performance_stream 
ON TABLE employee_performance_raw
append_only=false;

select * from employee_performance_raw;

--deduplicate logic to prevent duplicate values to be ingested
--creating a deduplicate table

CREATE OR REPLACE TABLE employee_performance_stage_dedup (
    employee_id STRING,
    evaluation_date DATE,
    performance_score INT,
    review_comments STRING,
    metadata_action STRING,
    record_hash string
);
-- STORE PROCEDURE script for implementing deduplication logic

CREATE OR REPLACE PROCEDURE employee_performance_sp()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Step 1: Insert deduplicated, cleaned records into staging
    MERGE INTO employee_performance_stage_dedup dst
    USING (
        SELECT
            employee_id,
            TRY_TO_DATE(evaluation_date) AS evaluation_date,
            CASE 
                WHEN performance_score = -100 THEN 100
                ELSE performance_score
            END AS performance_score,
            COALESCE(review_comments, 'No Comments') AS review_comments,
            METADATA$ACTION AS metadata_action,
            MD5(TO_JSON(OBJECT_CONSTRUCT(*))) AS record_hash
        FROM employee_performance_stream
        WHERE employee_id IS NOT NULL
          AND TRY_TO_DATE(evaluation_date) IS NOT NULL
          AND TRY_TO_NUMBER(performance_score) IS NOT NULL
          AND review_comments IS NOT NULL
    ) src
    ON dst.employee_id = src.employee_id 
       AND dst.record_hash = src.record_hash
    WHEN NOT MATCHED THEN
        INSERT (
            employee_id, evaluation_date, performance_score, review_comments, metadata_action, record_hash
        )
        VALUES (
            src.employee_id, src.evaluation_date, src.performance_score, src.review_comments, src.metadata_action, src.record_hash
        );
    -- Step 2: DELETE from final for all deleted/updated rows
    DELETE FROM employee_performance_final
    USING employee_performance_stage_dedup
    WHERE employee_performance_stage_dedup.metadata_action = 'DELETE'
      AND employee_performance_final.employee_id = employee_performance_stage_dedup.employee_id;
    -- Step 3: INSERT into final only new records without nulls
    INSERT INTO employee_performance_final (
        employee_id,
        evaluation_date,
        performance_score,
        review_comments
    )
    SELECT
        employee_id,
        evaluation_date,
        performance_score,
        review_comments
    FROM employee_performance_stage_dedup
    WHERE metadata_action = 'INSERT'
      AND employee_id IS NOT NULL
      AND evaluation_date IS NOT NULL
      AND performance_score IS NOT NULL
      AND review_comments IS NOT NULL;

    RETURN 'SUCCESS';
END;
$$;

--CREATING TASK TO CALL THE STORED PROCEDURE

CREATE OR REPLACE TASK employee_performance_task
WAREHOUSE = HR_WAREHOUSE
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('employee_performance_stream')
AS
CALL employee_performance_sp();

ALTER TASK EMPLOYEE_PERFORMANCE_TASK RESUME;

--ONETIME COPY STAtEMENT(with deduplication logic)

INSERT INTO employee_performance_stage_dedup (
    employee_id,
    evaluation_date,
    performance_score,
    review_comments,
    metadata_action,
    record_hash
)
SELECT
    employee_id::STRING AS employee_id,
    TRY_TO_DATE(evaluation_date::STRING) AS evaluation_date,
    CASE 
        WHEN TRY_TO_NUMBER(performance_score::STRING) = -100 THEN 100
        ELSE TRY_TO_NUMBER(performance_score::STRING)
    END AS performance_score,
    COALESCE(review_comments::STRING, 'No Comments') AS review_comments,
    'INSERT' AS metadata_action,
     MD5(TO_JSON(OBJECT_CONSTRUCT(*))) AS record_hash
FROM employee_performance_raw
WHERE employee_id IS NOT NULL
  AND evaluation_date IS NOT NULL
  AND performance_score IS NOT NULL;
  
--copying into the final table

INSERT INTO employee_performance_final (
    employee_id,
    evaluation_date,
    performance_score,
    review_comments
)
SELECT
    employee_id,
    evaluation_date,
    performance_score,
    review_comments
FROM employee_performance_stage_dedup
WHERE metadata_action = 'INSERT';

--table-> hr_attendance data
--stream
select* from employee_attendance_raw;

CREATE OR REPLACE STREAM employee_attendance_stream 
ON TABLE employee_attendance_raw
append_only=false;

--deduplicate logic table

CREATE OR REPLACE TABLE employee_attendance_stage_dedup (
    date_atten DATE,
    employee_id string,
    shift_time STRING,
    status string,
    metadata_action STRING,
    record_hash string);

--creating stored procedure 

CREATE OR REPLACE PROCEDURE employee_attendance_sp()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
MERGE INTO employee_attendance_stage_dedup dat
    USING (
        SELECT
            TRY_TO_DATE(date_atten) AS date_atten,
            employee_id,
            shift_time,
            status,
            METADATA$ACTION AS metadata_action,
            MD5(TO_JSON(OBJECT_CONSTRUCT(*))) AS record_hash
        FROM employee_attendance_stream
        WHERE employee_id IS NOT NULL
          AND date_atten IS NOT NULL
          AND shift_time IS NOT NULL
          AND status IS NOT NULL
    ) src
    ON dat.employee_id = src.employee_id 
       AND dat.record_hash = src.record_hash
    WHEN NOT MATCHED THEN
        INSERT (
           date_atten,employee_id,shift_time,status,metadata_action, record_hash)
        VALUES (
            src.date_atten,src.employee_id,src.shift_time, src.status, src.metadata_action, src.record_hash
        );
    -- Step 2: DELETE from final for all deleted/updated rows
    DELETE FROM employee_attendance_final
    USING employee_attendance_stage_dedup
    WHERE employee_attendance_stage_dedup.metadata_action = 'DELETE'
      AND employee_attendance_final.employee_id = employee_attendance_stage_dedup.employee_id
      AND employee_attendance_final.date_atten = employee_attendance_stage_dedup.date_atten
and employee_attendance_final.shift_time= employee_attendance_stage_dedup.shift_time
and employee_attendance_final.status = employee_attendance_stage_dedup.status;
    -- Step 3: INSERT into final only new records without nulls
    INSERT INTO employee_attendance_final (
        date_atten,
        employee_id,
        shift_time,
        status
    )
    SELECT
        date_atten,
        employee_id,
        shift_time,
        status
        FROM employee_attendance_stage_dedup
    WHERE metadata_action = 'INSERT'
        AND employee_id IS NOT NULL
        AND date_atten IS NOT NULL
        AND shift_time IS NOT NULL
      AND status IS NOT NULL;

    RETURN 'SUCCESS';
END;
$$;

--CREATING TASK TO CALL THE STORE PROCEDURE

CREATE OR REPLACE TASK employee_ATTENDANCE_task
WAREHOUSE = HR_WAREHOUSE
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('employee_attendance_stream')
AS
CALL employee_attendance_sp();

ALTER TASK EMPLOYEE_ATTENDANCE_TASK RESUME;

--ONE TIME COPY COMMAND STATEMENT

INSERT INTO employee_attendance_stage_dedup (
    date_atten,
    employee_id,
    shift_time,
    status,
    metadata_action,
    record_hash
)
SELECT
    TRY_TO_DATE(date_atten::STRING) as date_atten,
    employee_id::STRING AS employee_id,
    shift_time::string as shift_time,
    status::string as status,
    'INSERT' AS metadata_action,
     MD5(TO_JSON(OBJECT_CONSTRUCT(*))) AS record_hash
FROM employee_attendance_raw
WHERE employee_id IS NOT NULL
  AND date_atten IS NOT NULL
  AND shift_time IS NOT NULL
  AND status is not NULL;

select * from employee_attendance_stage_dedup;

--insert into final table

INSERT INTO employee_attendance_final(
 date_atten,
    employee_id,
    shift_time,
    status
)
select
date_atten,
    employee_id,
    shift_time,
    status
from employee_attendance_stage_dedup
WHERE metadata_action = 'INSERT';

select * from employee_attendance_final;
select * from employee_data_raw;

--table-> employee_data
--stream and task
--stream

CREATE OR REPLACE STREAM employee_data_stream 
ON TABLE employee_data_raw
append_only=false;

--deduplicate logic table

CREATE OR REPLACE TABLE employee_data_stage_dedup (
    employee_id string,
    first_name STRING,
    last_name STRING,
    dob date,
    email string,
    phone string,
    phone_extension varchar,
    hire_date string,
    department string,
    metadata_action STRING,
    record_hash string);

--creating stored procedure 
--to clean the phone number also and extract it in two columns

CREATE OR REPLACE PROCEDURE employee_data_sp()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
MERGE INTO employee_data_stage_dedup ddt
    USING (
        SELECT
            employee_id,
            first_name,
            last_name,
            TRY_TO_DATE(dob) AS dob,
            email,
             -- Clean phone: remove extension and country code
            REGEXP_REPLACE(
                REGEXP_REPLACE(TRIM(phone), 'x\\d{1,5}$', ''), 
            '^(001-|\\+1-)', ''
            ) AS phone,
        -- Extract extension: only the digits after 'x'
            REGEXP_SUBSTR(phone, 'x(\\d{1,5})$', 1, 1, 'e', 1) AS phone_extension,
            hire_date,
            department,
            METADATA$ACTION AS metadata_action,
            MD5(TO_JSON(OBJECT_CONSTRUCT(*))) AS record_hash
        FROM employee_data_stream
        WHERE employee_id IS NOT NULL
          AND first_name IS NOT NULL
          AND last_name IS NOT NULL
          AND dob IS NOT NULL
          AND email IS NOT NULL
          AND phone IS NOT NULL
          AND hire_date IS NOT NULL
          AND department IS NOT NULL
    ) src
    ON ddt.employee_id = src.employee_id 
       AND ddt.record_hash = src.record_hash
    WHEN NOT MATCHED THEN
        INSERT (
           employee_id,first_name,last_name,dob,email,phone,phone_extension,hire_date,department,metadata_action,record_hash)
        VALUES (
            src.employee_id,src.first_name,src.last_name,src.dob,src.email,src.phone,src.phone_extension,src.hire_date,src.department, src.metadata_action, src.record_hash
        );
    -- Step 2: DELETE from final for all deleted/updated rows
    DELETE FROM employee_data_final
    USING employee_data_stage_dedup
    WHERE employee_data_stage_dedup.metadata_action = 'DELETE'
      AND employee_data_final.employee_id = employee_data_stage_dedup.employee_id
      AND employee_data_final.dob = employee_data_stage_dedup.dob
and employee_data_final.email= employee_data_stage_dedup.email
and employee_data_final.phone = employee_data_stage_dedup.phone;
    -- Step 3: INSERT into final only new records without nulls
    INSERT INTO employee_data_final (
        employee_id,
            first_name,
            last_name,
            dob,
            email,
            phone,
            phone_extension,
            hire_date,
            department
    )
    SELECT
        employee_id,
            first_name,
            last_name,
            dob,
            email,
            phone,
            phone_extension,
            hire_date,
            department
        FROM employee_data_stage_dedup
    WHERE metadata_action = 'INSERT'
        AND employee_id IS NOT NULL
          AND first_name IS NOT NULL
          AND last_name IS NOT NULL
          AND dob IS NOT NULL
          AND email IS NOT NULL
          AND phone IS NOT NULL
          AND hire_date IS NOT NULL
          AND department IS NOT NULL;
    RETURN 'SUCCESS';
END;
$$;
call employee_data_sp();

--CREATING TASK TO CALL THE STORE PROCEDURE

CREATE OR REPLACE TASK employee_data_task
WAREHOUSE = HR_WAREHOUSE
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('employee_data_stream')
AS
CALL employee_data_sp();

ALTER TASK EMPLOYEE_DATA_TASK RESUME;

--ONE TIME COPY COMMAND STATEMENT

INSERT INTO employee_data_stage_dedup (
     employee_id,
     first_name,
     last_name,
     dob,
     email,
     phone,
     phone_extension,
     hire_date,
     department,
     metadata_action,
     record_hash
)
SELECT
      employee_id,
      first_name,
      last_name,
      TRY_TO_DATE(dob) AS dob,
      email,
             -- Clean phone: remove extension and country code
      REGEXP_REPLACE(
          REGEXP_REPLACE(TRIM(phone), 'x\\d{1,5}$', ''), 
      '^(001-|\\+1-)', ''
      ) AS phone,

        -- Extract extension: only the digits after 'x'
      REGEXP_SUBSTR(phone, 'x(\\d{1,5})$', 1, 1, 'e', 1) AS phone_extension,
      hire_date,
      department,
      'INSERT' AS metadata_action,
      MD5(TO_JSON(OBJECT_CONSTRUCT(*))) AS record_hash
FROM employee_data_raw
WHERE employee_id IS NOT NULL
          AND first_name IS NOT NULL
          AND last_name IS NOT NULL
          AND dob IS NOT NULL
          AND email IS NOT NULL
          AND phone IS NOT NULL
          AND hire_date IS NOT NULL
          AND department IS NOT NULL;

select * from employee_data_stage_dedup;

--insert into final table
 
INSERT INTO employee_data_final(
     employee_id,
     first_name,
     last_name,
     dob,
     email,
     phone,
     phone_extension,
     hire_date,
     department
)
select
     employee_id,
     first_name,
     last_name,
     dob,
     email,
     phone,
     phone_extension,
     hire_date,
     department
from employee_data_stage_dedup
WHERE metadata_action = 'INSERT';

--SALES AND MARKETING DEPARTMENT

use role sales_marketing_role;
use database fab_retailers;
use schema sales_marketing;
use warehouse sales_marketing_warehouse;

--TASK 4 -CLEANING THE DATA PESENT IN THE MARKETING DEPARTMENT 
--table-> store_locations_final
--stream and task
--stream

CREATE OR REPLACE STREAM marketing_campaign_stream 
ON TABLE marketing_campaign_raw
append_only=false;

select * from marketing_campaign_raw;

--deduplicate logic to remove deduplicate data
--creating a deduplicate table

CREATE OR REPLACE TABLE marketing_campaign_stage_dedup (
    campaign_id STRING,
    campaign_name STRING,
    start_date DATE,
    end_date DATE,
    budget INT,
    status STRING,
    store_id string,
    metadata_action STRING,
    record_hash string
);

-- STORE PROCEDURE script

CREATE OR REPLACE PROCEDURE marketing_campaign_sp()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Step 1: Insert deduplicated, cleaned records into staging
    MERGE INTO marketing_campaign_stage_dedup mst
    USING (
        SELECT
            campaign_id,
            campaign_name,
            TRY_TO_DATE(start_date) as start_date,
            TRY_TO_DATE(end_date) as end_date,
            budget,
            status,
            store_id,
            METADATA$ACTION AS metadata_action,
            MD5(TO_JSON(OBJECT_CONSTRUCT(*))) AS record_hash
        FROM marketing_campaign_stream
        WHERE campaign_id IS NOT NULL
          AND campaign_name IS NOT NULL
          AND start_date IS NOT NULL
          AND end_date IS NOT NULL
          AND budget IS NOT NULL
          AND status IS NOT NULL
    
    ) src
    ON mst.campaign_id = src.campaign_id 
       AND mst.record_hash = src.record_hash
    WHEN NOT MATCHED THEN
        INSERT (
            campaign_id, campaign_name, start_date,end_date, budget,status,store_id, metadata_action, record_hash
        )
        VALUES (
            src.campaign_id, src.campaign_name, src.start_date,src.end_date, src.budget,src.status,src.store_id, src.metadata_action, src.record_hash
        );
  -- Step 2: DELETE from final for all deleted/updated rows
    DELETE FROM marketing_campaign_final
    USING marketing_campaign_stage_dedup
    WHERE marketing_campaign_stage_dedup.metadata_action = 'DELETE'
      AND marketing_campaign_final.campaign_id = marketing_campaign_stage_dedup.campaign_id;
    -- Step 3: INSERT into final only new records without nulls
    INSERT INTO marketing_campaign_final (
          campaign_id,
          campaign_name,
          start_date,
          end_date,
          budget,
          status,
          store_id
    )
    SELECT
          campaign_id,
          campaign_name,
          start_date,
          end_date,
          budget,
          status,
          store_id
    FROM marketing_campaign_stage_dedup
    WHERE metadata_action = 'INSERT'
          AND campaign_id IS NOT NULL
          AND campaign_name IS NOT NULL
          AND start_date IS NOT NULL
          AND end_date IS NOT NULL
          AND budget IS NOT NULL
          AND status IS NOT NULL
          And store_id is not null;

    RETURN 'SUCCESS';
END;
$$;

call marketing_campaign_sp();

--CREATING TASK TO CALL THE STORED PROCEDURE

CREATE OR REPLACE TASK marketing_campaign_task
WAREHOUSE = SALES_MARKETING_WAREHOUSE
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('marketing_campaign_stream')
AS
CALL marketing_campaign_sp();

ALTER TASK marketing_campaign_TASK RESUME;

--ONETIME COPY STATEMENT

INSERT INTO marketing_campaign_stage_dedup (
    campaign_id,
    campaign_name,
    start_date,
    end_date,
    budget,
    status,
    store_id,
    metadata_action,
    record_hash
)
SELECT
    campaign_id,
    campaign_name,
    TRY_TO_DATE(start_date) as start_date,
    TRY_TO_DATE(end_date) as end_date,
    budget,
    status,
    store_id,
    'INSERT' AS metadata_action,
     MD5(TO_JSON(OBJECT_CONSTRUCT(*))) AS record_hash
FROM marketing_campaign_raw
WHERE campaign_id IS NOT NULL
      AND campaign_name IS NOT NULL
      AND start_date IS NOT NULL
      AND end_date IS NOT NULL
      AND budget IS NOT NULL
      AND status IS NOT NULL
      AND store_id IS NOT NULL
      ;

      
select * from marketing_campaign_stage_dedup;


--copying into the final table

INSERT INTO marketing_campaign_final (
    campaign_id,
    campaign_name,
    start_date,
    end_date,
    budget,
    status,
    store_id
)
SELECT
    campaign_id,
    campaign_name,
    start_date,
    end_date,
    budget,
    status,
    store_id
FROM marketing_campaign_stage_dedup
WHERE metadata_action = 'INSERT';

SELECT* FROM MARKETING_CAMPAIGN_FINAL;
 
--table->store_location_final
--stream and task
--stream

CREATE OR REPLACE STREAM store_locations_stream 
ON TABLE store_locations_raw
append_only=false;

select * from store_locations_raw;
--deduplicate logic as we dont have a unique id in this table
--creating a deduplicate table

CREATE OR REPLACE TABLE store_locations_stage_dedup (
    city string,
    contact_info string,
    extension string,
    region  string,
    store_id string,
    store_manager string,
    store_name string,
    metadata_action STRING,
    record_hash string
);
-- STORE PROCEDURE script

CREATE OR REPLACE PROCEDURE store_loctions_sp()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Step 1: Insert deduplicated, cleaned records into staging
    MERGE INTO store_locations_stage_dedup sst
    USING (
        SELECT
            city,
            -- Clean phone: remove extension and country code
            REGEXP_REPLACE(
                REGEXP_REPLACE(TRIM(contact_info), 'x\\d{1,5}$', ''), 
            '^(001-|\\+1-)', ''
            ) AS contact_info,
            -- Extract extension: only the digits after 'x'
            REGEXP_SUBSTR(contact_info, 'x(\\d{1,5})$', 1, 1, 'e', 1) AS extension,
            region,
            store_id,
            store_manager,
            store_name,
            METADATA$ACTION AS metadata_action,
            MD5(TO_JSON(OBJECT_CONSTRUCT(*))) AS record_hash
        FROM store_locations_stream
        WHERE city IS NOT NULL
          AND region IS NOT NULL
          AND store_id IS NOT NULL
          AND store_name IS NOT NULL
    ) src
    ON sst.store_id = src.store_id 
       AND sst.record_hash = src.record_hash
    WHEN NOT MATCHED THEN
        INSERT (
            city, contact_info, extension, region,store_id,store_manager,store_name, metadata_action, record_hash
        )
        VALUES (
            src.city, src.contact_info, src.extension, src.region,src.store_id,src.store_manager,src.store_name, src.metadata_action, src.record_hash
        );
    -- Step 2: DELETE from final for all deleted/updated rows
    DELETE FROM store_locations_final
    USING store_locations_stage_dedup
    WHERE store_locations_stage_dedup.metadata_action = 'DELETE'
      AND store_locations_final.store_id = store_locations_stage_dedup.store_id;
    -- Step 3: INSERT into final only new records without nulls
    INSERT INTO store_locations_final (
          city,
          contact_info, 
          extension,
          region,
          store_id,
          store_manager,
          store_name
          )
    SELECT
          city,
          contact_info, 
          extension,
          region,
          store_id,
          store_manager,
          store_name
    FROM store_locations_stage_dedup
    WHERE metadata_action = 'INSERT'
          And city IS NOT NULL
          AND region IS NOT NULL
          AND store_id IS NOT NULL
          AND store_name IS NOT NULL
          AND store_manager IS NOT NULL
          AND contact_info IS NOT NULL;
    RETURN 'SUCCESS';
END;
$$;


--CREATING TASK TO CALL THE STORED PROCEDURE

CREATE OR REPLACE TASK store_locations_task
WAREHOUSE = SALES_MARKETING_WAREHOUSE
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('store_locations_stream')
AS
CALL store_locations_sp();

ALTER TASK store_locations_TASK RESUME;

call store_locations_sp();
//ONETIME COPY STATEMENT
INSERT INTO store_locations_stage_dedup (
    city,
    contact_info, 
    extension,
    region,
    store_id,
    store_manager,
    store_name,
    metadata_action,
    record_hash
)
SELECT
    city,
    REGEXP_REPLACE(
        REGEXP_REPLACE(TRIM(contact_info), 'x\\d{1,5}$', ''), 
        '^(001-|\\+1-)', ''
        ) AS contact_info,
    -- Extract extension: only the digits after 'x'
    REGEXP_SUBSTR(contact_info, 'x(\\d{1,5})$', 1, 1, 'e', 1) AS extension,
    region,
    store_id,
    store_manager,
    store_name,
    'INSERT' AS metadata_action,
     MD5(TO_JSON(OBJECT_CONSTRUCT(*))) AS record_hash
FROM store_locations_raw
WHERE city IS NOT NULL
      AND region IS NOT NULL
      AND store_id IS NOT NULL
      AND store_name IS NOT NULL
      AND store_manager IS NOT NULL
      AND contact_info IS NOT NULL;

      
select * from store_locations_stage_dedup ;


--copying into the final table

INSERT INTO store_locations_final (
          city,
          contact_info, 
          extension,
          region,
          store_id,
          store_manager,
          store_name
          )
    SELECT
          city,
          contact_info, 
          extension,
          region,
          store_id,
          store_manager,
          store_name
    FROM store_locations_stage_dedup
    WHERE metadata_action = 'INSERT';



--table->store_sales_final
--stream and task
--stream

CREATE OR REPLACE STREAM store_sales_stream 
ON TABLE store_sales_raw
append_only=false;

select * from store_sales_raw;
--deduplicate logic as we dont have a unique id in this table
--creating a deduplicate table

CREATE OR REPLACE TABLE store_sales_stage_dedup (
    sales_id string,
    store_id string,
    product string,
    quantity_sold int,
    sale_price int,
    total_sale int,
    payment_method string,
    sale_date date,
    metadata_action STRING,
    record_hash string
);
-- STORE PROCEDURE script

CREATE OR REPLACE PROCEDURE store_sales_sp()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Step 1: Insert deduplicated, cleaned records into staging
    MERGE INTO store_sales_stage_dedup sss
    USING (
        SELECT
            sales_id,
            store_id,
            product,
            quantity_sold,
            sale_price,
            total_sale,
            payment_method,
            try_to_date(sale_date) as sale_date,
            METADATA$ACTION AS metadata_action,
            MD5(TO_JSON(OBJECT_CONSTRUCT(*))) AS record_hash
        FROM store_sales_stream
        WHERE sales_id IS NOT NULL
          AND product IS NOT NULL
          AND store_id IS NOT NULL
          AND quantity_sold IS NOT NULL
    ) src
    ON sss.sales_id = src.sales_id 
       AND sss.record_hash = src.record_hash
    WHEN NOT MATCHED THEN
        INSERT (
            sales_id, store_id, product, quantity_sold, sale_price, total_sale, payment_method, sale_date , metadata_action, record_hash
        )
        VALUES (
            src.sales_id, src.store_id, src.product, src.quantity_sold, src.sale_price,src.total_sale,src.payment_method, src.sale_date, src.metadata_action, src.record_hash
        );
    -- Step 2: DELETE from final for all deleted/updated rows
    DELETE FROM store_sales_final
    USING store_sales_stage_dedup
    WHERE store_sales_stage_dedup.metadata_action = 'DELETE'
      AND store_sales_final.sales_id = store_sales_stage_dedup.sales_id;
    -- Step 3: INSERT into final only new records without nulls
    INSERT INTO store_sales_final (
         sales_id,
         store_id,
         product,
         quantity_sold,
         sale_price,
         total_sale,
         payment_method,
         sale_date
          )
    SELECT
         sales_id,
         store_id,
         product,
         quantity_sold,
         sale_price,
         total_sale,
         payment_method,
         sale_date
         FROM store_sales_stage_dedup
    WHERE metadata_action = 'INSERT'
          And sales_id IS NOT NULL
          AND product IS NOT NULL
          AND store_id IS NOT NULL
          AND quantity_sold IS NOT NULL
          AND sale_price IS NOT NULL
          AND total_sale IS NOT NULL
          AND payment_method IS NOT NULL
          AND sale_date IS NOT NULL;
    RETURN 'SUCCESS';
END;
$$;

-- CREATING TASK TO CALL THE STORED PROCEDURE

CREATE OR REPLACE TASK store_sales_task
WAREHOUSE = SALES_MARKETING_WAREHOUSE
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('store_sales_stream')
AS
CALL store_sales_sp();

ALTER TASK store_sales_TASK RESUME;

--ONETIME COPY STATEMENT

INSERT INTO store_sales_stage_dedup (
         sales_id,
         store_id,
         product,
         quantity_sold,
         sale_price,
         total_sale,
         payment_method,
         sale_date,
         metadata_action,
         record_hash
)
SELECT
         sales_id,
         store_id,
         product,
         quantity_sold,
         sale_price,
         total_sale,
         payment_method,
         try_to_date(sale_date) as sale_date,
        'INSERT' AS metadata_action,
         MD5(TO_JSON(OBJECT_CONSTRUCT(*))) AS record_hash
FROM store_sales_raw
WHERE sales_id IS NOT NULL
          AND product IS NOT NULL
          AND store_id IS NOT NULL
          AND quantity_sold IS NOT NULL
          AND sale_price IS NOT NULL
          AND total_sale IS NOT NULL
          AND payment_method IS NOT NULL
          AND sale_date IS NOT NULL;

      
select * from store_sales_stage_dedup ;


--copying into the final table

INSERT INTO store_sales_final (
         sales_id,
         store_id,
         product,
         quantity_sold,
         sale_price,
         total_sale,
         payment_method,
         sale_date
          )
    SELECT
         sales_id,
         store_id,
         product,
         quantity_sold,
         sale_price,
         total_sale,
         payment_method,
         sale_date,
    FROM store_sales_stage_dedup
    WHERE metadata_action = 'INSERT';
