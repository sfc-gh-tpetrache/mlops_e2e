-- =====================================================================
-- Predictive Maintenance MLOps Demo -- compute setup
--
-- DEV matches the data scientist's notebook exactly:
--   warehouse    = AI_WH
--   compute pool = SYSTEM_COMPUTE_POOL_CPU   (built-in system pool, no CREATE)
--
-- PROD is isolated, dedicated compute (created below):
--   warehouse    = PDM_WH_PROD
--   compute pool = PDM_POOL_PROD             (CPU only)
--
-- Run with connection oregon_tp (role with CREATE WAREHOUSE / CREATE COMPUTE
-- POOL, e.g. ACCOUNTADMIN).
-- =====================================================================

-- ---------------------------------------------------------------------
-- DEV compute (match the notebook). AI_WH usually already exists; ensure it.
-- SYSTEM_COMPUTE_POOL_CPU is a built-in per-account pool used for SPCS model
-- serving / ML Jobs -- it does not need to be created.
-- ---------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS AI_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Shared DEV warehouse used by the predictive-maintenance notebook';

-- ---------------------------------------------------------------------
-- PROD compute (dedicated / isolated).
-- ---------------------------------------------------------------------
CREATE COMPUTE POOL IF NOT EXISTS PDM_POOL_PROD
  MIN_NODES = 1
  MAX_NODES = 2
  INSTANCE_FAMILY = CPU_X64_S
  AUTO_RESUME = TRUE
  AUTO_SUSPEND_SECS = 300
  COMMENT = 'Predictive maintenance demo - PROD compute pool (CPU)';

CREATE WAREHOUSE IF NOT EXISTS PDM_WH_PROD
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Predictive maintenance demo - PROD warehouse';

-- ---------------------------------------------------------------------
-- Optional cleanup: earlier iterations created per-env DEV/STAGING objects
-- that are no longer used (DEV now maps to AI_WH + SYSTEM_COMPUTE_POOL_CPU).
-- Uncomment to remove them.
-- ---------------------------------------------------------------------
-- DROP WAREHOUSE IF EXISTS PDM_WH_DEV;
-- DROP WAREHOUSE IF EXISTS PDM_WH_STAGING;
-- DROP COMPUTE POOL IF EXISTS PDM_POOL_DEV;
-- DROP COMPUTE POOL IF EXISTS PDM_POOL_STAGING;

-- ---------------------------------------------------------------------
-- Verify
-- ---------------------------------------------------------------------
SHOW WAREHOUSES LIKE 'AI_WH';
SHOW WAREHOUSES LIKE 'PDM_WH_PROD';
SHOW COMPUTE POOLS LIKE 'PDM_POOL_PROD';
