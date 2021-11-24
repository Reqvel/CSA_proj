import plotly.graph_objects as go
import pandas as pd

def show_locations(sql_connection, table_name):
    data_frame = pd.read_sql(f"""
        SELECT DISTINCT name, longitude, latitude, population
        FROM {table_name} 
        WHERE (population and longitude and latitude) IS NOT NULL""",
        sql_connection)

    fig = go.Figure(data=go.Scattergeo(
        lon = data_frame['longitude'],
        lat = data_frame['latitude'],
        text = data_frame['name'],
        mode = 'markers'
    ))

    fig.update_layout(
        title = 'Belarus locations',
        geo_scope='europe',
    )
    fig.show()