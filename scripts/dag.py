"""Unify the pipeline steps into a Snowflake Task Graph (DAG).

Each step is a Snowflake ML Job (via @remote) wired as a DAGTask:

    PREPARE_DATA -> [TRAIN_V1, TRAIN_V2] -> EVALUATE -> NOTIFY

Deploy (defines + registers the graph; does not run it):
    PDM_ENV=DEV python scripts/dag.py

Then trigger manually (EXECUTE TASK on the root) or on the schedule.
Tip: validate each step first with scripts/submit_step.py before deploying.
"""
import os
import sys
from datetime import timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from snowflake.core import Root, CreateMode  # noqa: E402
from snowflake.core.task.dagv1 import DAG, DAGTask, DAGOperation  # noqa: E402
from snowflake.ml.jobs import remote  # noqa: E402

from pdm_pipeline import common  # noqa: E402

ENV = os.getenv("PDM_ENV", "DEV")
cfg = common.get_config(ENV)
session = common.get_session()
session.use_warehouse(cfg.warehouse)
session.use_database(cfg.database)

STAGE = common.payload_stage_fqn(cfg)
session.sql(
    f"CREATE STAGE IF NOT EXISTS {STAGE} "
    f"DIRECTORY = (ENABLE = TRUE) ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')"
).collect()

_PKG = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src", "pdm_pipeline"))
_IMPORTS = [(_PKG, "pdm_pipeline")]


def _job(fn_name):
    """Build an @remote ML Job definition that runs one pipeline step main()."""
    env = ENV  # captured into the remote payload

    def job_fn():
        import importlib
        mod = importlib.import_module(f"pdm_pipeline.{fn_name}")
        return mod.main(env=env)

    # Name must start with a letter (used to derive the job's service DNS name),
    # so set it BEFORE applying @remote (the decorator captures __name__).
    job_fn.__name__ = f"{fn_name}_job"
    job_fn.__qualname__ = job_fn.__name__
    return remote(cfg.compute_pool, stage_name=STAGE, session=session, imports=_IMPORTS)(job_fn)


def build_and_deploy(run: bool = False):
    dag = DAG(
        "PDM_TRAINING_PIPELINE",
        schedule=timedelta(days=1),
        warehouse=cfg.warehouse,
    )
    with dag:
        prepare = DAGTask("PREPARE_DATA", definition=_job("prepare_data"))
        train_v1 = DAGTask("TRAIN_V1", definition=_job("train_v1"))
        train_v2 = DAGTask("TRAIN_V2", definition=_job("train_v2"))
        evaluate = DAGTask("EVALUATE", definition=_job("evaluate"))
        # NOTIFY runs last. It can also be marked as the graph FINALIZER
        # (DAGTask(..., is_finalizer=True)) so it runs even if a step fails.
        notify = DAGTask("NOTIFY", definition=_job("notify"))

        prepare >> [train_v1, train_v2]
        train_v1 >> evaluate
        train_v2 >> evaluate
        evaluate >> notify

    root = Root(session)
    schema_ref = root.databases[cfg.database].schemas[f"{common.SCHEMA}_MODEL_REGISTRY"]
    dag_op = DAGOperation(schema_ref)
    dag_op.deploy(dag, mode=CreateMode.or_replace)
    print(f"Deployed DAG PDM_TRAINING_PIPELINE to {cfg.database}.{common.SCHEMA}_MODEL_REGISTRY "
          f"(env={cfg.env}, pool={cfg.compute_pool}, wh={cfg.warehouse})")

    if run:
        dag_op.run(dag)
        print("Triggered DAG run PDM_TRAINING_PIPELINE. Watch it in Snowsight -> "
              "Monitoring -> Task History, or query the task_history table function.")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--run", action="store_true",
                   help="Trigger the DAG immediately after deploying it.")
    args = p.parse_args()
    build_and_deploy(run=args.run)
