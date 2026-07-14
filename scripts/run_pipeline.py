"""Local entrypoint: run the whole pipeline in-process for debugging.

Runs each step sequentially in the current environment (no ML Jobs / DAG).
Useful for fast iteration before submitting steps remotely or deploying the DAG.

    PDM_ENV=DEV python scripts/run_pipeline.py
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from pdm_pipeline import prepare_data, train_v1, train_v2, evaluate, notify  # noqa: E402


def main():
    env = os.getenv("PDM_ENV", "DEV")
    dataset_name = prepare_data.main(env=env)
    v1 = train_v1.main(env=env, dataset_name=dataset_name)
    v2 = train_v2.main(env=env, dataset_name=dataset_name)
    summary = evaluate.main(env=env, versions=[v1, v2])
    notify.main(env=env, summary=summary)
    print("Pipeline complete.")


if __name__ == "__main__":
    main()
