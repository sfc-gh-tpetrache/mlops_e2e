import plotly.graph_objects as go

def plot_machine_data(viz_df, machine_id, start_date=None, end_date=None):
    """
    Plots machine sensor data and failure events using Plotly.
    
    Args:
        viz_df (pd.DataFrame): The input dataframe.
        machine_id (str/int): The ID of the machine to plot.
        start_date (str/datetime, optional): Start range for the X-axis.
        end_date (str/datetime, optional): End range for the X-axis.
    """
    # --- 1. Filter Data ---
    # Default to min/max dates if none provided
    if start_date is None: start_date = viz_df['DATE'].min()
    if end_date is None: end_date = viz_df['DATE'].max()
    
    plot_df = viz_df[
        (viz_df['DATE'].between(start_date, end_date)) & 
        (viz_df['MACHINE_ID'] == machine_id)
    ].copy()
    
    # --- 2. Initialize the Plotly Figure ---
    fig = go.Figure()
    
    # --- 3. Add Sensor Traces ---
    sensor_columns = [
        'SENSOR_1_DAILY_AVERAGE',
        'SENSOR_2_DAILY_AVERAGE',
        'SENSOR_3_DAILY_AVERAGE'
    ]
    
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c'] 
    
    for i, col in enumerate(sensor_columns):
        if col in plot_df.columns:
            fig.add_trace(go.Scatter(
                x=plot_df['DATE'],
                y=plot_df[col],
                mode='lines',
                name=col.replace('_', ' ').title(),
                line=dict(color=colors[i]),
                marker=dict(size=8)
            ))
    
    # --- 4. Identify and Add Vertical Lines for Failures ---
    failure_dates = plot_df[plot_df['FAILURE'] == 1]['DATE']
    failure_dates_list = failure_dates.tolist() 
    
    for date in failure_dates:
        fig.add_vline(
            x=date,
            line_width=2,
            line_dash="dash",
            line_color="#e32636"
        )
    
    # --- 4b. Add Annotation to Indicate Failure Line Type ---
    if failure_dates_list:
        first_failure_date = failure_dates_list[0]
        fig.add_annotation(
            x=first_failure_date,
            yref="paper", 
            y=0.95, 
            text="FAILURE",
            showarrow=True,
            arrowhead=2,
            arrowsize=1,
            arrowwidth=1,
            arrowcolor="#e32636",
            bgcolor="rgba(255, 255, 255, 0.7)",
            bordercolor="#e32636",
            borderwidth=1,
            font=dict(color="#e32636", size=10, weight='bold')
        )
    
    # --- 5. Configure Layout ---
    fig.update_layout(
        title={
            'text': f"Sensor Data: Machine {machine_id}",
            'y':0.95,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'
        },
        xaxis_title="Date",
        yaxis_title="Daily Average Sensor Value",
        hovermode="x unified",
        template="plotly_white",
        legend_title_text='Sensor Readings',
        font=dict(family="Arial, sans-serif", size=12, color="Black"),
        margin=dict(l=40, r=40, t=100, b=40)
    )
    
    return fig