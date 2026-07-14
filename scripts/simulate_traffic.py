"""Canary step 3 (Task 8d): simulate inference traffic through the gateway.

Sends many small POST requests to the gateway's /predict endpoint so the 90/10
traffic split is exercised (each request is one routing decision). Includes
pre-drift and post-drift feature rows, and passes MACHINE_ID/DATE via
extra_columns so auto-capture logs carry IDs for the later ground-truth join.

    SNOWFLAKE_CONNECTION_NAME=oregon_tp PDM_ENV=PROD python scripts/simulate_traffic.py
"""
import os
import sys
import json
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from pdm_pipeline import common  # noqa: E402

FEATURES = [f"SENSOR_{s}_DAILY_AVERAGE_LAG_{l}" for s in (1, 2, 3) for l in (1, 2, 3)]
ID_COLS = ["MACHINE_ID", "DATE"]
N_REQUESTS = int(os.getenv("PDM_TRAFFIC_REQUESTS", "120"))
ROWS_PER_REQUEST = int(os.getenv("PDM_TRAFFIC_ROWS", "20"))
TOKEN_NAME = "PDM_TRAFFIC_TOKEN"


def main():
    import requests

    cfg = common.get_config(os.getenv("PDM_ENV", "PROD"))
    session = common.get_session()
    session.use_warehouse(cfg.warehouse)
    session.use_database(cfg.database)

    reg_schema = common.registry_schema(cfg)
    fv_table = f'{cfg.database}.{common.SCHEMA}_FEATURE_STORE."MACHINE_SENSORS_LAG_FEATURES$1"'

    ingress = session.sql(
        f'DESC GATEWAY {reg_schema}.PDM_GATEWAY ->> SELECT "ingress_url" FROM $1'
    ).collect()[0][0]
    endpoint = f"https://{ingress}/predict"
    print(f"Gateway endpoint: {endpoint}", flush=True)

    # Fresh PAT for the calls (no network policy on this account, so PAT works).
    try:
        session.sql(f"ALTER USER REMOVE PROGRAMMATIC ACCESS TOKEN {TOKEN_NAME}").collect()
    except Exception:
        pass
    # This account has no network policy, and PAT auth requires one by default.
    # MINS_TO_BYPASS_NETWORK_POLICY_REQUIREMENT waives that requirement for this token.
    pat = session.sql(
        f"ALTER USER ADD PROGRAMMATIC ACCESS TOKEN {TOKEN_NAME} "
        f"MINS_TO_BYPASS_NETWORK_POLICY_REQUIREMENT = 60"
    ).collect()[0]["token_secret"]
    headers = {"Authorization": f'Snowflake Token="{pat}"', "Content-Type": "application/json"}

    cols = ", ".join(FEATURES + ID_COLS)
    # Build a pool of rows: pre-drift (normal) and post-drift (shifted) windows.
    def fetch(lo, hi, n):
        q = (f'SELECT {cols} FROM {fv_table} '
             f"WHERE DATE BETWEEN '{lo}' AND '{hi}' AND {FEATURES[0]} IS NOT NULL "
             f"ORDER BY RANDOM() LIMIT {n}")
        df = session.sql(q).to_pandas()
        df["DATE"] = df["DATE"].astype(str)
        return df

    pre = fetch("2025-03-01", "2025-03-31", N_REQUESTS * ROWS_PER_REQUEST // 2)
    post = fetch("2025-08-01", "2025-10-31", N_REQUESTS * ROWS_PER_REQUEST // 2)
    print(f"Fetched pre-drift={len(pre)} post-drift={len(post)} rows", flush=True)

    ok = 0
    codes = {}
    for i in range(N_REQUESTS):
        src = pre if i % 2 == 0 else post
        start = (i // 2 * ROWS_PER_REQUEST) % max(1, (len(src) - ROWS_PER_REQUEST))
        batch = src.iloc[start:start + ROWS_PER_REQUEST]
        split = json.loads(batch.to_json(orient="split"))
        payload = {
            "dataframe_split": {
                "index": split["index"],
                "columns": split["columns"],
                "data": split["data"],
            },
            "extra_columns": ID_COLS,
        }
        try:
            r = requests.post(endpoint, json=payload, headers=headers, timeout=60)
            codes[r.status_code] = codes.get(r.status_code, 0) + 1
            if r.status_code == 200:
                ok += 1
            elif i < 3:
                print(f"  req {i}: {r.status_code} {r.text[:160]}", flush=True)
        except Exception as e:
            print(f"  req {i} error: {e}", flush=True)
        time.sleep(0.2)

    print(f"Sent {N_REQUESTS} requests -> {ok} OK. status codes: {codes}", flush=True)
    try:
        session.sql(f"ALTER USER REMOVE PROGRAMMATIC ACCESS TOKEN {TOKEN_NAME}").collect()
    except Exception:
        pass
    print("Traffic simulation done.", flush=True)


if __name__ == "__main__":
    main()
