SET MY_USER = CURRENT_USER();
--Roles
CREATE OR REPLACE ROLE DEV_ROLE;
GRANT ROLE DEV_ROLE TO ROLE SYSADMIN;
GRANT ROLE DEV_ROLE TO USER IDENTIFIER($MY_USER);

GRANT EXECUTE TASK ON ACCOUNT TO ROLE DEV_ROLE;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE DEV_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE DEV_ROLE;
--Database
CREATE OR REPLACE DATABASE DEV_DB;
GRANT OWNERSHIP ON DATABASE DEV_DB TO ROLE DEV_ROLE;
--Warehouse
CREATE OR REPLACE WAREHOUSE DEV_WH WAREHOUSE_SIZE = XSMALL, AUTO_SUSPEND = 300, AUTO_RESUME= TRUE;
GRANT OWNERSHIP ON WAREHOUSE DEV_WH TO ROLE DEV_ROLE;
--Schema
CREATE OR REPLACE SCHEMA DEV_SCHEMA;
USE SCHEMA DEV_SCHEMA;
CREATE OR REPLACE STAGE DATA_RAW_STAGE
    URL = 's3://sfquickstarts/data-engineering-with-snowpark-python/'
;