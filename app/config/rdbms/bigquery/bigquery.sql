-- BigQuery Connection Test Queries

-- Get current project ID
SELECT @@project_id AS current_project;

-- Get current dataset (if applicable, though usually not set session-wide like a schema)
-- SELECT @@dataset_id; -- This is not a standard session variable, usually specified in queries.

-- List datasets in the current project (requires appropriate permissions)
-- SELECT schema_name FROM INFORMATION_SCHEMA.SCHEMATA;

-- Get current user
SELECT SESSION_USER() AS current_session_user;

-- Basic query to ensure connectivity and query execution
SELECT 1 + 1 AS test_calculation;

-- Show version (less direct than other DBs, but can use UDF or check release notes)
-- BigQuery updates are continuous. SELECT @@version often gives general Google SQL version info.
SELECT @@version as bigquery_version_info;

-- Example: Create a temporary table, insert, and select (demonstrates DDL and DML capabilities)
/*
CREATE TEMP TABLE bq_connection_test_temp (
  id INT64,
  description STRING
);
INSERT INTO bq_connection_test_temp (id, description) VALUES (1, 'Connection test successful');
SELECT * FROM bq_connection_test_temp;
DROP TABLE bq_connection_test_temp;
*/

-- Note: For actual connection testing from an application, it's best to execute a simple, non-destructive query like 'SELECT 1'.
-- The queries above are for interactive exploration or more detailed checks. 