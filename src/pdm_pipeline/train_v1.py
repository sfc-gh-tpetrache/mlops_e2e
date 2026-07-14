"""Pipeline step 2a: train V1 = LogisticRegression (baseline).

Standalone ML Job or DAG task. Trains on the prepared Dataset and registers
model version V1 in the env's model registry with a consistent `predict`
signature (output: FAILURE_IN_1_DAY_PREDICTION) so it can be A/B compared with V2.
"""
import argparse
import json


def main(env: str = "DEV", dataset_name: str | None = None) -> str:
    import pandas as pd  # noqa: F401
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import precision_recall_fscore_support
    from snowflake.snowpark import functions as F
    from snowflake.ml.registry import Registry
    from snowflake.ml.model.model_signature import infer_signature
    from snowflake.ml.experiment.experiment_tracking import ExperimentTracking

    from pdm_pipeline import common

    cfg = common.get_config(env)
    session = common.get_session()
    session.use_warehouse(cfg.warehouse)
    session.use_database(cfg.database)
    session.use_schema(f"{cfg.database}.{common.SCHEMA}_MODEL_REGISTRY")

    from snowflake.ml.dataset import load_dataset
    ds = load_dataset(session, common.dataset_name(cfg), common.DATASET_VERSION)
    df = ds.read.to_snowpark_dataframe()

    train_df = df.filter(F.col("DATE").between(common.TRAIN_START, common.TRAIN_END)).to_pandas()
    test_df = df.filter(F.col("DATE").between(common.TEST_START, common.TEST_END)).to_pandas()

    unused = [common.TARGET, "MACHINE_ID", "DATE"]
    features = [c for c in train_df.columns if c not in unused]

    # Experiment Tracking: log this run's params + metrics to a Snowflake Experiment
    exp = ExperimentTracking(session=session)
    exp.set_experiment(common.EXPERIMENT_NAME)

    model = LogisticRegression(solver="liblinear", random_state=42)
    with exp.start_run(run_name="LogisticRegression_V1"):
        exp.log_param("algorithm", "LogisticRegression")
        exp.log_param("solver", "liblinear")
        model.fit(train_df[features], train_df[common.TARGET])
        y_pred = model.predict(test_df[features])
        precision, recall, f1, _ = precision_recall_fscore_support(
            test_df[common.TARGET], y_pred, average="weighted", zero_division=0.0
        )
        metrics = {"precision": float(precision), "recall": float(recall), "f1": float(f1)}
        exp.log_metrics(metrics)

    reg = Registry(session, database_name=cfg.database,
                   schema_name=f"{common.SCHEMA}_MODEL_REGISTRY",
                   options={"enable_monitoring": True})
    sdf = ds.read.to_snowpark_dataframe()
    signature = infer_signature(
        input_data=sdf.select(features).limit(100),
        output_data=sdf.select(common.TARGET)
        .rename({common.TARGET: f"{common.TARGET}_PREDICTION"}).limit(100),
    )
    mv = reg.log_model(
        model,
        model_name=common.MODEL_NAME,
        version_name=common.VERSION_V1,
        metrics=metrics,
        comment="LogisticRegression baseline (V1) to predict machine failures",
        conda_dependencies=["scikit-learn"],
        signatures={"predict": signature},
        sample_input_data=sdf.select(features).limit(100),
        options={"relax_version": True, "enable_explainability": False},
        target_platforms=["WAREHOUSE", "SNOWPARK_CONTAINER_SERVICES"],
    )
    print(json.dumps({"version": mv.version_name, "algorithm": "LogisticRegression", "metrics": metrics}))
    return mv.version_name


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--env", default="DEV")
    p.add_argument("--dataset-name", default=None)
    a = p.parse_args()
    __return__ = main(env=a.env, dataset_name=a.dataset_name)
