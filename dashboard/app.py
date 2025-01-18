# Dash - Dashboard library
import dash
from dash import dcc, html, Input, Output
# Plotly.express - graphing, visualizastion library
import plotly.express as px
import duckdb
import pandas as pd


# The 'with' statement ensures: The duckdb connection (db_connection) is opened when the block starts. The connection is automatically closed when the block finishes, regardless of success or failure.
with duckdb.connect("../air_quality.db", read_only=True) as db_connection:
    # .fetchdf() is a duckdb method that convert the query into a pandas df
    df = db_connection.execute(
        "SELECT * FROM presentation.air_quality_data"
    ).fetchdf()
    daily_stats_df = db_connection.execute(
        "SELECT * FROM presentation.daily_air_quality_stats"
    ).fetchdf()
    latest_values_df = db_connection.execute(
        "SELECT * FROM presentation.latest_param_values_per_location"
    ).fetchdf()

    'co', 'no', 'no2', 'nox', 'o3', 'pm25'

def map_figure():
    map_fig = px.scatter_mapbox(
        latest_values_df, 
        lat = "lat", 
        lon= "lon", 
        hover_name = "location", 
        hover_data = {
            "lat" : False,
            "lon" : False,
            "datetime" : True,
            "co" : True,
            "no" : True, 
            "no2" : True, 
            "nox" : True, 
            "o3" : True,
            "pm25" : True
        },
        zoom = 7.0
    )

    map_fig.update_layout(
        mapbox_style="open-street-map", 
        height = 800, 
        title = "Air Quality Monitoring Locations"
    )

    return map_fig

def line_figure():
    line_fig = px.line(
        daily_stats_df[daily_stats_df["parameter"] == "nox"].sort_values(by="measurement_date"), 
        x="measurement_date",
        y="average_value",
        title="Plot Over Time NOX Levels ", 
    )

    return line_fig

def box_figure():
    box_fig = px.box(
        daily_stats_df[daily_stats_df["parameter"] == "nox"].sort_values(by="weekday_number"), 
        x="weekday",
        y="average_value",
        title="Distribution of NOX Levels by Weekday"
    )

    return box_fig




app = dash.Dash(__name__)

# We'll define different visual features, like map_figure(), and add them to our layout. These will be passed into our layout as a list
app.layout = html.Div([
    dcc.Tabs([
        # Page 1.
        dcc.Tab(
            label="Sensor Locations", 
            children=dcc.Graph(id="map-view", figure=map_figure())
        ),
        # Page 2. which has two plots
        dcc.Tab(
            label="Parameter Plots",
            children=[
                dcc.Graph(id="line-plot", figure=line_figure()),
                dcc.Graph(id="box-plot", figure=box_figure())
            ]
        )
    ])
])



if __name__ == "__main__":
    app.run_server(debug=True)