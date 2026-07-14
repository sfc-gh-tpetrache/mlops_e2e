"""Pipeline step 1: prepare data.

Builds the Feature Store objects (entity + lag feature view) and generates the
training Dataset, mirroring the notebook's feature-engineering steps.

Runs two ways:
  - Standalone ML Job:  submit_file("prepare_data.py", ...) with __return__.
  - DAG task:           imported by scripts/dag.py and wrapped with @remote.
"""
import argparse
import json


def main(env: str = "DEV") -> str:
    """Create/refresh the feature view and generate the training dataset.

    Returns the fully-qualified dataset name (passed to downstream train steps).
    """
    from snowflake.snowpark import functions as F
    from snowflake.ml.feature_store import (
        FeatureStore, CreationMode, Entity, FeatureView,
    )
    from snowflake.ml.dataset import Dataset  # noqa: F401 (ensures ml.dataset available)

    from pdm_pipeline import common

    cfg = common.get_config(env)
    session = common.get_session()
    session.use_warehouse(cfg.warehouse)
    session.use_database(cfg.database)

    database, schema = cfg.database, common.SCHEMA

    # Reproducibility: drop any pre-existing model + its inference services so the
    # pipeline owns a fresh model each run (train steps then create V1/V2 cleanly).
    reg_schema = common.registry_schema(cfg)
    try:
        for r in session.sql(f"SHOW SERVICES IN SCHEMA {reg_schema}").collect():
            row = r.as_dict()
            if common.MODEL_NAME in (row.get("managing_object_name") or ""):
                session.sql(f"DROP SERVICE IF EXISTS {reg_schema}.{row['name']}").collect()
    except Exception:
        pass
    try:
        session.sql(f"DROP MODEL IF EXISTS {reg_schema}.{common.MODEL_NAME}").collect()
    except Exception:
        pass

    fs = FeatureStore(
        session=session,
        database=database,
        name=common.feature_store_name(),
        default_warehouse=cfg.warehouse,
        creation_mode=CreationMode.CREATE_IF_NOT_EXIST,
    )

    # Daily aggregation of raw sensor data
    daily_sensor_data = (
        session.table(f"{database}.{schema}.MACHINE_SENSORS")
        .with_column("DATE", F.date_trunc("DAY", "SENSOR_TIMESTAMP").cast("date"))
        .group_by("MACHINE_ID", "DATE")
        .agg(
            F.avg("SENSOR_1").alias("SENSOR_1_DAILY_AVERAGE"),
            F.avg("SENSOR_2").alias("SENSOR_2_DAILY_AVERAGE"),
            F.avg("SENSOR_3").alias("SENSOR_3_DAILY_AVERAGE"),
        )
    )

    # Lag features
    lag_features = daily_sensor_data.analytics.compute_lag(
        cols=common.SENSORS,
        lags=list(range(1, common.N_LAGS + 1)),
        order_by=["DATE"],
        group_by=["MACHINE_ID"],
    ).drop(common.SENSORS)

    # Entity + feature view (idempotent)
    machine_entity = Entity(name=common.ENTITY_NAME, join_keys=common.JOIN_KEYS,
                            desc="Unique Machine ID")
    fs.register_entity(machine_entity)

    fv = FeatureView(
        name=common.FEATURE_VIEW_NAME,
        entities=[machine_entity],
        feature_df=lag_features,
        timestamp_col="DATE",
        refresh_freq="1 minute",
        refresh_mode="INCREMENTAL",
        desc="Lag Features for Machine Sensors",
    )
    fv = fs.register_feature_view(feature_view=fv, version=common.FEATURE_VIEW_VERSION,
                                  overwrite=True)

    # Spine: machines x dates with 1-day-ahead failure label
    start, end = common.DATASET_WINDOW
    machines_df = (
        session.table(f"{database}.{schema}.MACHINE_SENSORS")
        .with_column("DATE", F.date_trunc("DAY", "SENSOR_TIMESTAMP").cast("date"))
        .filter(F.col("DATE").between(start, end))
        .select("MACHINE_ID", "DATE").distinct()
    )
    machine_failures = (
        session.table(f"{database}.{schema}.MACHINE_FAILURES")
        .with_column("DATE", F.date_add(F.col("DATE"), F.lit(-1)))
        .rename({"FAILURE": common.TARGET})
    )
    spine_df = (
        machines_df.join(machine_failures, how="left", on=["MACHINE_ID", "DATE"])
        .order_by("MACHINE_ID", "DATE")
        .fillna(0, subset=[common.TARGET])
    )

    dataset_name = common.dataset_name(cfg)

    # Idempotent: drop an existing dataset of the same name/version so re-runs work.
    try:
        from snowflake.ml.dataset import load_dataset
        existing = load_dataset(session, dataset_name, common.DATASET_VERSION)
        existing.delete()
    except Exception:
        pass

    dataset = fs.generate_dataset(
        name=dataset_name,
        spine_df=spine_df,
        features=[fv],
        version=common.DATASET_VERSION,
        spine_timestamp_col="DATE",
        spine_label_cols=[common.TARGET],
        desc="Training dataset to predict machine failures.",
    )
    n = dataset.read.to_snowpark_dataframe().count()
    print(json.dumps({"dataset": dataset_name, "version": common.DATASET_VERSION,
                      "rows": n, "env": cfg.env}))
    return dataset_name


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--env", default="DEV")
    a = p.parse_args()
    __return__ = main(env=a.env)
