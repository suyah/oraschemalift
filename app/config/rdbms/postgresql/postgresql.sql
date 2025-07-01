-- PostgreSQL Connection Test Queries
SELECT version();
SELECT current_database();
SELECT current_schema();
SHOW server_version;
SHOW search_path;
SELECT current_user;
SELECT session_user;
-- List a few roles (limit for brevity, requires permissions)
-- SELECT rolname FROM pg_roles LIMIT 5;
-- List a few databases (limit for brevity, requires permissions)
-- SELECT datname FROM pg_database WHERE datistemplate = false LIMIT 5; 