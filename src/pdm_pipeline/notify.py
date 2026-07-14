"""Pipeline finalizer: notify.

Emits a run summary. In the DAG this is wired as the FINALIZER task so it runs
after all steps complete (or fail). Kept dependency-free so it can also run on a
warehouse task.
"""
import argparse
import json


def main(env: str = "DEV", summary: str | None = None) -> str:
    from pdm_pipeline import common

    cfg = common.get_config(env)
    msg = {
        "pipeline": "PDM_TRAINING_PIPELINE",
        "env": cfg.env,
        "database": cfg.database,
        "summary": json.loads(summary) if summary else None,
        "status": "complete",
    }
    print(json.dumps(msg))
    # Hook point: SYSTEM$SEND_EMAIL / notification integration could go here.
    return json.dumps(msg)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--env", default="DEV")
    p.add_argument("--summary", default=None)
    a = p.parse_args()
    __return__ = main(env=a.env, summary=a.summary)
