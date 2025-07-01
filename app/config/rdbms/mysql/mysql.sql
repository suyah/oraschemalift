-- MySQL Connection Test Queries
SELECT VERSION() AS mysql_version;
SELECT DATABASE() AS current_database;
SELECT USER() AS current_user;
SELECT CONNECTION_ID() AS connection_id;
SELECT @@hostname AS hostname;
SELECT @@port AS port;
SELECT @@character_set_database AS charset;
SELECT @@collation_database AS collation;
SELECT @@sql_mode AS sql_mode;
SELECT @@time_zone AS time_zone;
-- Show engine status
SHOW ENGINES;
-- Show some basic status (limit for brevity)
-- SHOW STATUS LIKE 'Uptime%';
-- Show some variables (commented to keep test lightweight)
-- SHOW VARIABLES LIKE 'version%'; 