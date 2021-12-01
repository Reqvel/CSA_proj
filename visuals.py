from pandas.core.frame import DataFrame
import plotly.express as px

def show_locations(data_frame: DataFrame, mapbox_token: str) -> None:
    fig = px.scatter_mapbox(
        data_frame, 
        lat='lat', 
        lon='lon',
        hover_name='location_name',
        hover_data=['population', 'id'],
        color='color',
        # size='size',
        zoom=6)

    fig.update_layout(
        title = 'Belarus locations',
        mapbox_style='dark', mapbox_accesstoken=mapbox_token,
        geo_scope='europe',
    )

    fig.update_layout(margin={"r":0,"t":100,"l":0,"b":0})

    fig.show()