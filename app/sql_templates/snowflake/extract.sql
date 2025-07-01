-- Snowflake DDL & privilege extraction script
-- {database} will be substituted.

-- 1) Full database DDL (incl. schemas, tables, etc.)
SELECT GET_DDL('DATABASE', '{database}', TRUE);

-- 2) Grants – object-side
SHOW GRANTS ON ACCOUNT;
SHOW GRANTS ON DATABASE {database};
-- Add more SHOW GRANTS as needed by tooling.

-- 3) Principal-side
SHOW ROLES;
-- (client loops over result, calling SHOW GRANTS TO ROLE <role_name>)
SHOW USERS;
-- (client loops over result, calling SHOW GRANTS TO USER <user_name>); 