-- Oracle Connection Test Queries
SELECT banner FROM v$version WHERE banner LIKE 'Oracle Database%';
SELECT instance_name FROM v$instance;
SELECT name FROM v$database;
SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AS current_schema FROM DUAL;
SELECT SYS_CONTEXT('USERENV', 'SESSION_USER') AS session_user FROM DUAL;
SELECT DBTIMEZONE AS db_timezone FROM DUAL;
SELECT SESSIONTIMEZONE AS session_timezone FROM DUAL;
-- Show some NLS parameters
SELECT parameter, value FROM NLS_DATABASE_PARAMETERS WHERE parameter IN ('NLS_CHARACTERSET', 'NLS_NCHAR_CHARACTERSET', 'NLS_DATE_FORMAT');
-- Check if the connected user can create a table (simple test, might fail if user lacks permissions)
-- This is commented out by default as it's a DDL and might not always be desired for a 'test' query.
-- CREATE TABLE connection_test_temp (id NUMBER);
-- DROP TABLE connection_test_temp; 