USE ROLE ACCOUNTADMIN;

-- ---------------------------------------------------------
-- 1. Databases & Schemas (Must come first)
-- ---------------------------------------------------------

-- Create Database for AI Development
CREATE DATABASE IF NOT EXISTS AI_DEMOS;
ALTER DATABASE AI_DEMOS SET LOG_LEVEL = INFO;
ALTER DATABASE AI_DEMOS SET METRIC_LEVEL = ALL;
ALTER DATABASE AI_DEMOS SET TRACE_LEVEL = ALWAYS;

-- Create Database for Snowflake Intelligence Agents
CREATE DATABASE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE;
CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_INTELLIGENCE.AGENTS;

-- ---------------------------------------------------------
-- 2. Core Infrastructure & Logging
-- ---------------------------------------------------------

-- Create a warehouse
CREATE WAREHOUSE IF NOT EXISTS AI_WH 
    WITH WAREHOUSE_SIZE='X-SMALL' 
    AUTO_SUSPEND=60 
    AUTO_RESUME=TRUE;

-- Create an Event Table for Logging (Now that DB exists)
CREATE EVENT TABLE IF NOT EXISTS AI_DEMOS.PUBLIC.AI_LOGS;

-- Activate the Event Table for the Account
ALTER DATABASE AI_DEMOS SET EVENT_TABLE = AI_DEMOS.PUBLIC.AI_LOGS;

-- Create Image Repository for Docker Images (SPCS)
CREATE IMAGE REPOSITORY IF NOT EXISTS AI_DEMOS.PUBLIC.AI_IMAGES;

-- ---------------------------------------------------------
-- 3. Networking & Integrations
-- ---------------------------------------------------------

-- Allow cross-region access for Cortex
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- Create Network Policy to allow public access (for testing/demo purposes)
-- Network Policy is required to use Programmatic Access Tokens
CREATE NETWORK RULE IF NOT EXISTS allow_public_access
  MODE = INGRESS
  TYPE = IPV4
  VALUE_LIST = ('0.0.0.0/0');

CREATE NETWORK POLICY IF NOT EXISTS allow_public_access_policy
  ALLOWED_NETWORK_RULE_LIST = ('allow_public_access');

-- Create External Access (Egress)
CREATE NETWORK RULE IF NOT EXISTS ai_external_access_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('0.0.0.0:80', '0.0.0.0:443');

CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS ai_external_access_integration
  ALLOWED_NETWORK_RULES = (ai_external_access_rule)
  ENABLED = true;

-- Email Integration
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS ai_email_int
  TYPE=EMAIL
  ENABLED=TRUE;

-- Create the API integration with Github
CREATE API INTEGRATION IF NOT EXISTS AI_GITHUB_API_INTEGRATION
   API_PROVIDER = git_https_api
   API_ALLOWED_PREFIXES = ('https://github.com/')
   API_USER_AUTHENTICATION = (
      TYPE = snowflake_github_app
   )
   ENABLED = TRUE;

-- ---------------------------------------------------------
-- 4. Role Creation & Grants
-- ---------------------------------------------------------

CREATE ROLE IF NOT EXISTS AI_DEVELOPER;

-- --- Database Objects ---
GRANT CREATE DATABASE ON ACCOUNT TO ROLE AI_DEVELOPER;
GRANT ALL ON DATABASE AI_DEMOS TO ROLE AI_DEVELOPER;
GRANT USAGE ON DATABASE SNOWFLAKE_INTELLIGENCE TO ROLE AI_DEVELOPER;

-- Grant broad permissions on the public schema
GRANT ALL ON SCHEMA AI_DEMOS.PUBLIC TO ROLE AI_DEVELOPER;

-- --- Logging Access ---
GRANT ALL ON TABLE AI_DEMOS.PUBLIC.AI_LOGS TO ROLE AI_DEVELOPER;

-- --- Image Repository ---
GRANT READ, WRITE ON IMAGE REPOSITORY AI_DEMOS.PUBLIC.AI_IMAGES TO ROLE AI_DEVELOPER;

-- --- Compute & Warehouse ---
GRANT ALL ON WAREHOUSE AI_WH TO ROLE AI_DEVELOPER;

-- Allow developer to create service endpoints
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE AI_DEVELOPER;

-- --- Integrations ---
GRANT USAGE ON INTEGRATION ai_external_access_integration TO ROLE AI_DEVELOPER;
GRANT USAGE ON INTEGRATION ai_email_int TO ROLE AI_DEVELOPER;
GRANT USAGE ON INTEGRATION AI_GITHUB_API_INTEGRATION TO ROLE AI_DEVELOPER;

-- --- Cortex & AI Features ---
-- Access to Cortex Functions (Complete, Translate, etc.)
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE AI_DEVELOPER;
-- Access to Cortex Analyst monitoring
GRANT APPLICATION ROLE snowflake.cortex_analyst_requests_admin TO ROLE AI_DEVELOPER;
-- Access to PyPi Packages
GRANT DATABASE ROLE SNOWFLAKE.PYPI_REPOSITORY_USER TO ROLE AI_DEVELOPER;
-- Access to usage metrics in Snowflake views
GRANT DATABASE ROLE SNOWFLAKE.USAGE_VIEWER TO ROLE AI_DEVELOPER;

-- --- Snowflake Intelligence Agents ---
GRANT USAGE, CREATE AGENT ON SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS TO ROLE AI_DEVELOPER;

-- ---------------------------------------------------------
-- Create new User
-- ---------------------------------------------------------
CREATE USER IF NOT EXISTS ai_developer_01
    PASSWORD             = '<password>'            -- Set a temporary password
    LOGIN_NAME           = 'ai_developer_01'   -- The name used to login
    DISPLAY_NAME         = 'AI Developer 01'   -- UI display name
    FIRST_NAME           = '<firstname>'
    LAST_NAME            = '<lastname>'
    EMAIL                = '<email>'
    DEFAULT_ROLE         = AI_DEVELOPER        -- Auto-selects role on login
    DEFAULT_WAREHOUSE    = AI_WH               -- Auto-selects warehouse on login
    MUST_CHANGE_PASSWORD = TRUE;               -- Enforces password change on first login

-- ---------------------------------------------------------
-- Grant the Role
-- ---------------------------------------------------------
-- Explicitly grant the role to the user so they can use it
GRANT ROLE AI_DEVELOPER TO USER ai_developer_01;
ALTER USER ai_developer_01 SET NETWORK_POLICY = allow_public_access_policy;