"""Run a SINGLE pipeline step as a Snowflake ML Job (test steps independently).

Each step is submitted with submit_directory so the whole pdm_pipeline package
is uploaded; the step file is the entrypoint and returns its result via __return__.

    PDM_ENV=DEV python scripts/submit_step.py prepare_data
    PDM_ENV=DEV python scripts/submit_step.py train_v1
    PDM_ENV=DEV python scripts/submit_step.py train_v2
    PDM_ENV=DEV python scripts/submit_step.py evaluate

Once each step passes here, unify them with scripts/dag.py.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from pdm_pipeline import common  # noqa: E402

STEPS = {
    "prepare_data": "pdm_pipeline/prepare_data.py",
    "train_v1": "pdm_pipeline/train_v1.py",
    "train_v2": "pdm_pipeline/train_v2.py",
    "evaluate": "pdm_pipeline/evaluate.py",
}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("step", choices=list(STEPS))
    p.add_argument("--env", default=os.getenv("PDM_ENV", "DEV"))
    args = p.parse_args()

    from snowflake.ml.jobs import submit_directory

    cfg = common.get_config(args.env)
    session = common.get_session()
    session.use_warehouse(cfg.warehouse)
    session.use_database(cfg.database)
    session.sql(
        f"CREATE STAGE IF NOT EXISTS {common.payload_stage_fqn(cfg)} "
        f"DIRECTORY = (ENABLE = TRUE) ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')"
    ).collect()

    src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
    job = submit_directory(
        src_dir,
        compute_pool=cfg.compute_pool,
        entrypoint=STEPS[args.step],
        stage_name=common.payload_stage_fqn(cfg),
        args=["--env", args.env],
        session=session,
    )
    print(f"Submitted {args.step} as ML Job {job.id} on {cfg.compute_pool}")
    print("Status:", job.status)
    result = job.result()  # blocks until done
    print("Result:", result)
    return result


if __name__ == "__main__":
    main()
