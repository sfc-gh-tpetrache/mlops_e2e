"""Canary step 1: deploy V1 and V2 as SPCS inference services (Task 8b).

Both services get ingress enabled + Auto Capture (required for gateway A/B
monitors). Auto Capture is supported because V1/V2 were created after
2026-01-23. The autocapture setting is immutable per service.

    SNOWFLAKE_CONNECTION_NAME=oregon_tp PDM_ENV=PROD python scripts/deploy_services.py
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from pdm_pipeline import common  # noqa: E402

SERVICES = [
    (common.VERSION_V1, "PDM_SERVICE_V1"),  # LogisticRegression (baseline)
    (common.VERSION_V2, "PDM_SERVICE_V2"),  # XGBoost (candidate)
]


def main():
    from snowflake.ml.registry import Registry

    cfg = common.get_config(os.getenv("PDM_ENV", "PROD"))
    session = common.get_session()
    session.use_warehouse(cfg.warehouse)
    session.use_database(cfg.database)
    session.use_schema(f"{cfg.database}.{common.SCHEMA}_MODEL_REGISTRY")

    reg = Registry(session, database_name=cfg.database,
                   schema_name=f"{common.SCHEMA}_MODEL_REGISTRY")
    model = reg.get_model(common.MODEL_NAME)

    for version, service_name in SERVICES:
        mv = model.version(version)
        print(f"Deploying {service_name} from {version} on {cfg.compute_pool} ...", flush=True)
        mv.create_service(
            service_name=service_name,
            service_compute_pool=cfg.compute_pool,
            ingress_enabled=True,
            autocapture=True,
            gpu_requests=None,
            min_instances=1,
            max_instances=1,
        )
        print(f"  {service_name} created.", flush=True)

    print("Endpoints:")
    for _, service_name in SERVICES:
        rows = session.sql(f"SHOW ENDPOINTS IN SERVICE {service_name}").collect()
        for r in rows:
            d = r.as_dict()
            print(f"  {service_name}: {d.get('name')} -> {d.get('ingress_url')}", flush=True)
    print("Service deployment done.")


if __name__ == "__main__":
    main()
