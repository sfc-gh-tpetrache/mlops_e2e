-- Canary step 2 (Task 8c): Snowflake Gateway with 90/10 traffic split.
-- V1 (LogisticRegression, baseline) gets 90%, V2 (XGBoost, candidate) gets 10%.
-- Weights must sum to 100; max 5 endpoints. Stable ingress URL for the lifetime
-- of the gateway. Requires the two services to exist with ingress enabled.

CREATE OR REPLACE GATEWAY AI_DEMOS_PROD.IOT_PREDICTIVE_MAINTENANCE_MODEL_REGISTRY.PDM_GATEWAY
  FROM SPECIFICATION $$
    spec:
      type: traffic_split
      split_type: custom
      targets:
        - type: endpoint
          value: AI_DEMOS_PROD.IOT_PREDICTIVE_MAINTENANCE_MODEL_REGISTRY.PDM_SERVICE_V1!inference
          weight: 90
        - type: endpoint
          value: AI_DEMOS_PROD.IOT_PREDICTIVE_MAINTENANCE_MODEL_REGISTRY.PDM_SERVICE_V2!inference
          weight: 10
  $$;

-- Stable public endpoint (call https://<ingress_url>/predict):
DESC GATEWAY AI_DEMOS_PROD.IOT_PREDICTIVE_MAINTENANCE_MODEL_REGISTRY.PDM_GATEWAY
  ->> SELECT "ingress_url" AS endpoint FROM $1;

-- --- Progressive rollout (Task 8e): shift traffic, then promote or rollback ---
-- 50/50:
-- ALTER GATEWAY AI_DEMOS_PROD.IOT_PREDICTIVE_MAINTENANCE_MODEL_REGISTRY.PDM_GATEWAY
--   FROM SPECIFICATION $$
--     spec: {type: traffic_split, split_type: custom, targets: [
--       {type: endpoint, value: AI_DEMOS_PROD.IOT_PREDICTIVE_MAINTENANCE_MODEL_REGISTRY.PDM_SERVICE_V1!inference, weight: 50},
--       {type: endpoint, value: AI_DEMOS_PROD.IOT_PREDICTIVE_MAINTENANCE_MODEL_REGISTRY.PDM_SERVICE_V2!inference, weight: 50}]}
--   $$;
-- Promote V2 (0/100):  ...weights 0 / 100.  Rollback: 100 / 0.
