-- Greenplum Connection Test Queries
SELECT version(); -- Similar to PostgreSQL
SELECT current_database();
SELECT current_schema();
SHOW search_path;
SELECT current_user;
SELECT session_user;
-- Greenplum specific system catalog queries if needed for more detail
-- Example: List segments (requires superuser or appropriate permissions)
-- SELECT hostname, port, content as segment_index, role FROM gp_segment_configuration; 