"""Shared config + helpers for the predictive-maintenance ML pipeline.

Everything is parameterized by ENVIRONMENT so the same code runs in DEV and PROD:
  - DEV  -> AI_DEMOS       + AI_WH   + SYSTEM_COMPUTE_POOL_CPU  (matches the notebook)
  - PROD -> AI_DEMOS_PROD  + PDM_WH_PROD + PDM_POOL_PROD        (isolated compute)

Select the environment with the PDM_ENV env var (default DEV).
"""
import os
from dataclasses import dataclass


# ---------------------------------------------------------------------------
# Environment configuration
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class EnvConfig:
    env: str
    database: str
    warehouse: str
    compute_pool: str


_ENVS = {
    "DEV": EnvConfig("DEV", "AI_DEMOS", "AI_WH", "SYSTEM_COMPUTE_POOL_CPU"),
    "PROD": EnvConfig("PROD", "AI_DEMOS_PROD", "PDM_WH_PROD", "PDM_POOL_PROD"),
}


def get_config(env: str | None = None) -> EnvConfig:
    env = (env or os.getenv("PDM_ENV", "DEV")).upper()
    if env not in _ENVS:
        raise ValueError(f"Unknown PDM_ENV={env!r}; expected one of {list(_ENVS)}")
    return _ENVS[env]


# ---------------------------------------------------------------------------
# Constants (shared across pipeline steps) - match the notebook
# ---------------------------------------------------------------------------
SCHEMA = "IOT_PREDICTIVE_MAINTENANCE"
MODEL_NAME = "PREDICTIVE_MAINTENANCE_MODEL"
FEATURE_VIEW_NAME = "MACHINE_SENSORS_LAG_FEATURES"
FEATURE_VIEW_VERSION = "1"
ENTITY_NAME = "MACHINE"
JOIN_KEYS = ["MACHINE_ID"]
TARGET = "FAILURE_IN_1_DAY"
N_LAGS = 3
SENSORS = ["SENSOR_1_DAILY_AVERAGE", "SENSOR_2_DAILY_AVERAGE", "SENSOR_3_DAILY_AVERAGE"]

# Dataset windows (match the notebook)
TRAIN_START, TRAIN_END = "2025-01-04", "2025-02-28"
TEST_START, TEST_END = "2025-03-01", "2025-04-01"
DATASET_WINDOW = ("2025-01-04", "2025-04-01")

# Model versions -> algorithm mapping (the canary A/B pair)
VERSION_V1 = "V1"   # LogisticRegression (baseline)
VERSION_V2 = "V2"   # XGBoost (candidate)

# Feature Store Dataset version (fixed so steps load deterministically)
DATASET_VERSION = "v1"

# Experiment Tracking (per-run metrics/params logged to a Snowflake Experiment)
EXPERIMENT_NAME = "PREDICTIVE_MAINTENANCE_EXPERIMENT"

# ML Job payload stage (per env: <DB>.<SCHEMA>_MODEL_REGISTRY.PDM_PAYLOAD_STAGE)
PAYLOAD_STAGE = "PDM_PAYLOAD_STAGE"


def registry_schema(cfg: EnvConfig) -> str:
    return f"{cfg.database}.{SCHEMA}_MODEL_REGISTRY"


def dataset_name(cfg: EnvConfig) -> str:
    """Feature Store Dataset object (versioned, carries FS -> model lineage)."""
    return f"{registry_schema(cfg)}.PREDICTIVE_MAINTENANCE_DATASET"


def feature_store_name() -> str:
    return f"{SCHEMA}_FEATURE_STORE"


def payload_stage_fqn(cfg: EnvConfig) -> str:
    return f"{registry_schema(cfg)}.{PAYLOAD_STAGE}"


# ---------------------------------------------------------------------------
# Dual-mode session (Snowsight / VS Code Remote-Dev reuse active session;
# local falls back to a named connection).
# ---------------------------------------------------------------------------
def get_session():
    try:
        from snowflake.snowpark.context import get_active_session
        return get_active_session()
    except Exception:
        from snowflake.snowpark import Session
        return Session.builder.config(
            "connection_name", os.getenv("SNOWFLAKE_CONNECTION_NAME", "oregon_tp")
        ).create()
