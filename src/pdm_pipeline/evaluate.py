"""Pipeline step 3: evaluate V1 and V2 side by side.

Reads both registered versions' metrics from the registry and picks a winner by
F1. Returns a JSON summary used by notify (and, later, to seed the canary).
"""
import argparse
import json


def main(env: str = "DEV", versions: list[str] | None = None) -> str:
    from snowflake.ml.registry import Registry
    from pdm_pipeline import common

    cfg = common.get_config(env)
    session = common.get_session()
    session.use_warehouse(cfg.warehouse)
    session.use_database(cfg.database)

    versions = versions or [common.VERSION_V1, common.VERSION_V2]
    reg = Registry(session, database_name=cfg.database,
                   schema_name=f"{common.SCHEMA}_MODEL_REGISTRY")
    model = reg.get_model(common.MODEL_NAME)

    results = {}
    for v in versions:
        mv = model.version(v)
        results[v] = mv.show_metrics() or {}

    def f1(v):
        return float(results.get(v, {}).get("f1", 0.0))

    winner = max(versions, key=f1)
    summary = {"env": cfg.env, "metrics": results, "winner": winner}
    print(json.dumps(summary))
    return json.dumps(summary)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--env", default="DEV")
    a = p.parse_args()
    __return__ = main(env=a.env)
