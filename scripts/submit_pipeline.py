"""Run the whole pipeline as sequential Snowflake ML Jobs in ONE session.

Validates every step end-to-end as an ML Job (one auth for the whole chain).
Order: prepare_data -> train_v1 -> train_v2 -> evaluate -> notify.

    PDM_ENV=DEV python scripts/submit_pipeline.py

For orchestrated/scheduled execution use scripts/dag.py instead.
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from pdm_pipeline import common  # noqa: E402

STEPS = [
    ("prepare_data", "pdm_pipeline/prepare_data.py"),
    ("train_v1", "pdm_pipeline/train_v1.py"),
    ("train_v2", "pdm_pipeline/train_v2.py"),
    ("evaluate", "pdm_pipeline/evaluate.py"),
    ("notify", "pdm_pipeline/notify.py"),
]


def main():
    from snowflake.ml.jobs import submit_directory

    env = os.getenv("PDM_ENV", "DEV")
    cfg = common.get_config(env)
    session = common.get_session()
    session.use_warehouse(cfg.warehouse)
    session.use_database(cfg.database)
    session.sql(
        f"CREATE STAGE IF NOT EXISTS {common.payload_stage_fqn(cfg)} "
        f"DIRECTORY = (ENABLE = TRUE) ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')"
    ).collect()

    src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
    only = os.getenv("PDM_STEPS")  # optional comma-separated subset, e.g. "train_v1,train_v2"
    steps = STEPS
    if only:
        wanted = {s.strip() for s in only.split(",")}
        steps = [(n, e) for n, e in STEPS if n in wanted]
    for name, entrypoint in steps:
        job = submit_directory(
            src_dir,
            compute_pool=cfg.compute_pool,
            entrypoint=entrypoint,
            stage_name=common.payload_stage_fqn(cfg),
            args=["--env", env],
            session=session,
        )
        print(f"[{name}] submitted {job.id} (status={job.status})", flush=True)
        result = job.result()  # blocks until done; raises on failure
        print(f"[{name}] DONE -> {result}", flush=True)

    print(f"PIPELINE COMPLETE ({env})", flush=True)


if __name__ == "__main__":
    main()
