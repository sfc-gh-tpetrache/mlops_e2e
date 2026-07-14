"""Generate the PROD environment (AI_DEMOS_PROD) and its data.

Reproducible, code-first provisioning of the PROD predictive-maintenance data.
Uses the SAME generation parameters as the notebook (via demo_functions.setup),
so PROD data is structurally identical but an independent random draw.

Runs in Snowflake:
  - In Snowsight Workspaces / VS Code Remote-Dev: an active session is reused.
  - Locally / CI: builds a Snowpark session from a named connection
    (SNOWFLAKE_CONNECTION_NAME, default "oregon_tp").

Usage:
    SNOWFLAKE_CONNECTION_NAME=oregon_tp python infra/generate_prod_data.py
"""
import os
import sys

# Make the vendored demo_functions package importable when run from repo root.
_SRC = os.path.join(os.path.dirname(__file__), "..", "src")
if os.path.abspath(_SRC) not in sys.path:
    sys.path.insert(0, os.path.abspath(_SRC))

import demo_functions

DATABASE = os.getenv("PDM_PROD_DATABASE", "AI_DEMOS_PROD")
SCHEMA = os.getenv("PDM_SCHEMA", "IOT_PREDICTIVE_MAINTENANCE")
# Serving/append window (matches the notebook's scoring window).
APPEND_START = os.getenv("PDM_APPEND_START", "2025-04-01")
APPEND_END = os.getenv("PDM_APPEND_END", "2026-07-10")


def get_session():
    try:
        from snowflake.snowpark.context import get_active_session
        return get_active_session()
    except Exception:
        from snowflake.snowpark import Session
        return Session.builder.config(
            "connection_name", os.getenv("SNOWFLAKE_CONNECTION_NAME", "oregon_tp")
        ).create()


def main():
    session = get_session()
    print(f"Provisioning PROD: database={DATABASE}, schema={SCHEMA}")

    # Creates AI_DEMOS_PROD (+ schemas), demo data, and initial 2025-01-01..2025-04-01 window.
    demo_functions.setup(session, SCHEMA, database=DATABASE)

    # Extend MACHINE_SENSORS / MACHINE_FAILURES through the serving window.
    print(f"Appending data window {APPEND_START} -> {APPEND_END}")
    demo_functions.generate_machine_data(
        session, SCHEMA,
        start_date=APPEND_START,
        end_date=APPEND_END,
        mode="append",
        database=DATABASE,
    )

    # Sanity output
    for tbl in ("MACHINE_SENSORS", "MACHINE_FAILURES", "DIM_MACHINES"):
        cnt = session.sql(f"SELECT COUNT(*) AS N FROM {DATABASE}.{SCHEMA}.{tbl}").collect()[0]["N"]
        print(f"  {DATABASE}.{SCHEMA}.{tbl}: {cnt} rows")
    print("PROD data generation done.")


if __name__ == "__main__":
    main()
