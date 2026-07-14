import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from snowflake.snowpark import functions as F
from datetime import date

def create_timeseries_dataframe(start_date: str, end_date: str, feature_drift_date: str) -> pd.DataFrame:
    """
    Create a realistic looking time series DataFrame with sensor data.
    
    Parameters:
    -----------
    start_date : str
        Start date in YYYY-MM-DD format
    end_date : str
        End date in YYYY-MM-DD format
    feature_drift_date : str
        Date when anomaly behavior changes in YYYY-MM-DD format
    
    Returns:
    --------
    pd.DataFrame
        DataFrame with columns: SENSOR_TIMESTAMP, SENSOR_1, SENSOR_2, SENSOR_3
    """
    # Parse dates
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    drift_date = datetime.strptime(feature_drift_date, '%Y-%m-%d')
    
    # Generate hourly timestamps with random minutes
    timestamps = []
    current = start.replace(minute=0, second=0, microsecond=0)
    
    while current <= end:
        # Random minute between 0 and 59
        random_minute = np.random.randint(0, 60)
        timestamp = current.replace(minute=random_minute)
        timestamps.append(timestamp)
        current += timedelta(hours=1)
    
    n_rows = len(timestamps)
    
    # Initialize sensor values with random walk
    # Before drift_date: bounded between 0 and 1
    # After drift_date: bounded between 1 and 3
    # Start with random values between 0 and 1
    sensor_1 = np.zeros(n_rows)
    sensor_2 = np.zeros(n_rows)
    sensor_3 = np.zeros(n_rows)
    
    sensor_1[0] = np.random.uniform(0, 1)
    sensor_2[0] = np.random.uniform(0, 1)
    sensor_3[0] = np.random.uniform(0, 1)
    
    # Generate bounded random walk for normal periods
    noise_scale = 0.05  # Standard deviation for random walk
    
    def apply_bounds(value, min_val, max_val):
        """Reflect value back into bounds if it goes outside"""
        if value < min_val:
            return 2 * min_val - value  # Reflect
        elif value > max_val:
            return 2 * max_val - value  # Reflect
        return value
    
    for i in range(1, n_rows):
        # Determine bounds based on whether timestamp is before or after drift_date
        if timestamps[i] < drift_date:
            # Before drift: keep between 0 and 1
            min_val, max_val = 0, 1
        else:
            # After drift: keep between 1 and 3
            min_val, max_val = 1, 3
        
        sensor_1[i] = apply_bounds(sensor_1[i-1] + np.random.normal(0, noise_scale), min_val, max_val)
        sensor_2[i] = apply_bounds(sensor_2[i-1] + np.random.normal(0, noise_scale), min_val, max_val)
        sensor_3[i] = apply_bounds(sensor_3[i-1] + np.random.normal(0, noise_scale), min_val, max_val)
    
    # Identify monthly anomaly periods
    anomaly_periods = []
    current_month = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    while current_month <= end:
        # Find a random day in the month for anomaly start
        # Get the last day of the month
        if current_month.month == 12:
            next_month = current_month.replace(year=current_month.year + 1, month=1)
        else:
            next_month = current_month.replace(month=current_month.month + 1)
        
        last_day = (next_month - timedelta(days=1)).day
        anomaly_day = np.random.randint(1, last_day + 1)
        anomaly_start = current_month.replace(day=anomaly_day)
        
        # Anomaly duration: 1 or 3 days
        anomaly_duration_days = np.random.choice([3, 4])
        anomaly_end = min(anomaly_start + timedelta(days=int(anomaly_duration_days)), end)
        
        # Only add if within our date range
        if anomaly_start <= end:
            anomaly_periods.append((anomaly_start, anomaly_end))
        
        # Move to next month
        current_month = next_month
    
    # Apply anomalies (values respect the drift-based bounds: 0-1 before drift, 1-3 after drift)
    for anomaly_start, anomaly_end in anomaly_periods:
        # Find indices within anomaly period
        anomaly_indices = [
            i for i, ts in enumerate(timestamps)
            if anomaly_start <= ts <= anomaly_end
        ]
        
        if not anomaly_indices:
            continue
        
        # Determine if this anomaly is before or after feature_drift_date
        is_before_drift = anomaly_start < drift_date
        
        # Apply anomaly pattern with gradual changes over the period
        num_anomaly_points = len(anomaly_indices)
        for local_idx, idx in enumerate(anomaly_indices):
            # Progress from 0 to 1 over the anomaly period (gradual build-up)
            progress = local_idx / (num_anomaly_points - 1) if num_anomaly_points > 1 else 1.0
            
            # Determine bounds based on timestamp
            if timestamps[idx] < drift_date:
                min_val, max_val = 0, 1
            else:
                min_val, max_val = 1, 3
            
            if is_before_drift:
                # Before drift: SENSOR_1 and SENSOR_2 drop, SENSOR_3 increases
                # Gradually move from normal range (0 to 1) to extreme values within bounds
                # SENSOR_1 and SENSOR_2 move toward 0, SENSOR_3 moves toward 1
                sensor_1[idx] = np.clip((1 - progress) * 0.5 + np.random.normal(0, 0.1), min_val, max_val)
                sensor_2[idx] = np.clip((1 - progress) * 0.5 + np.random.normal(0, 0.1), min_val, max_val)
                sensor_3[idx] = np.clip(0.5 + progress * 0.5 + np.random.normal(0, 0.1), min_val, max_val)
            else:
                # After drift: SENSOR_1 drops, SENSOR_2 and SENSOR_3 increase
                # SENSOR_1 moves toward 1, SENSOR_2 and SENSOR_3 move toward 3
                sensor_1[idx] = np.clip(3 - progress * 2 + np.random.normal(0, 0.2), min_val, max_val)
                sensor_2[idx] = np.clip(1 + progress * 2 + np.random.normal(0, 0.2), min_val, max_val)
                sensor_3[idx] = np.clip(1 + progress * 2 + np.random.normal(0, 0.2), min_val, max_val)
    
    # Create DataFrame
    df = pd.DataFrame({
        'SENSOR_TIMESTAMP': timestamps,
        'SENSOR_1': sensor_1,
        'SENSOR_2': sensor_2,
        'SENSOR_3': sensor_3
    })
    
    # Sort by timestamp to ensure proper ordering
    df = df.sort_values('SENSOR_TIMESTAMP').reset_index(drop=True)
    
    return df, pd.DataFrame(anomaly_periods, columns=['ANOMALY_START','ANOMALY_END'])

def generate_machine_data(session, schema, start_date, end_date, mode='overwrite', database='AI_DEMOS'):
    sensor_values_df = (
        session.table(f'{database}.{schema}._MACHINE_SENSORS_DEMO_DATA')
            .filter(F.col('SENSOR_TIMESTAMP').between(start_date, end_date))
    )
    
    sensor_values_df.write.save_as_table(table_name=f'{database}.{schema}.MACHINE_SENSORS', mode=mode)
    
    targets_df = (
        session.table(f'{database}.{schema}._MACHINE_FAILURES_DEMO_DATA')
            .filter(F.col('DATE').between(start_date, end_date))
    )
    
    targets_df.write.save_as_table(table_name=f'{database}.{schema}.MACHINE_FAILURES', mode=mode)


def setup(session, schema, database='AI_DEMOS'):
    # Remove existing data
    print('Removing existing data ...')
    session.sql(f"CREATE DATABASE IF NOT EXISTS {database};").collect()
    session.sql(f"CREATE OR REPLACE SCHEMA {database}.{schema};").collect()
    session.sql(f"CREATE OR REPLACE SCHEMA {database}.{schema}_FEATURE_STORE;").collect()
    session.sql(f"CREATE OR REPLACE SCHEMA {database}.{schema}_MODEL_REGISTRY;").collect()

    # Create demo data
    print('Creating demo data ...')
    machines = 200
    start_date = '2025-01-01'
    #end_date = '2025-10-31'
    end_date = date.today().isoformat()
    feature_drift_date = '2025-06-01'
    
    machine_ids = [f'MACHINE_{id:04d}' for id in range(machines)]
    dim_machines = pd.DataFrame(machine_ids, columns=['MACHINE_ID'])
    dim_machines = session.write_pandas(
        dim_machines, 
        database=database,
        schema=schema,
        table_name='DIM_MACHINES',
        auto_create_table=True,
        overwrite=True
        )
    
    session.sql(f'ALTER TABLE {database}.{schema}.DIM_MACHINES ADD PRIMARY KEY (MACHINE_ID);').collect()

    sensor_values_df = []
    anomaly_periods_df = []
    for machine_id in machine_ids:
        df, anomaly_periods = create_timeseries_dataframe(
            start_date=start_date,
            end_date=end_date,
            feature_drift_date=feature_drift_date
        )
        df['MACHINE_ID'] = machine_id
        sensor_values_df.append(df)
        anomaly_periods['MACHINE_ID'] = machine_id
        anomaly_periods_df.append(anomaly_periods)
    sensor_values_df = pd.concat(sensor_values_df)[['MACHINE_ID','SENSOR_TIMESTAMP','SENSOR_1','SENSOR_2','SENSOR_3']]
    sensor_values_df = sensor_values_df.reset_index(drop=True)
    anomaly_periods_df = pd.concat(anomaly_periods_df)[['MACHINE_ID','ANOMALY_START','ANOMALY_END']]
    
    targets_df = anomaly_periods_df[['MACHINE_ID','ANOMALY_END']].rename(columns={'ANOMALY_END':'DATE'})
    targets_df['DATE'] = targets_df['DATE'].dt.date
    targets_df['FAILURE'] = 1
    targets_df = targets_df.reset_index(drop=True)

    # Persist demo data
    session.write_pandas(
        df=sensor_values_df,
        database=database,
        schema=schema,
        table_name='_MACHINE_SENSORS_DEMO_DATA', 
        auto_create_table=True, 
        use_logical_type=True, 
        overwrite=True
    )
    
    session.write_pandas(
        df=targets_df,
        database=database,
        schema=schema,
        table_name='_MACHINE_FAILURES_DEMO_DATA', 
        auto_create_table=True, 
        use_logical_type=True, 
        overwrite=True
    )

    # Create initial data
    generate_machine_data(session, schema, start_date='2025-01-01', end_date='2025-04-01', database=database)
    session.use_schema(f'{database}.{schema}_MODEL_REGISTRY')
    print('Demo Setup done!')