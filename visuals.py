from pandas.core.frame import DataFrame
import plotly.express as px

def show_locations(data_frame: DataFrame, mapbox_token: str, hours: float) -> None:
    fig = px.scatter_mapbox(
        data_frame, 
        lat='lat', 
        lon='lon',
        hover_name='location_name',
        hover_data=['population', 'id'],
        color='color_info',
        size='size',
        size_max=10,
        opacity=1,
        zoom=6)

    fig.update_layout(
        title = f'Belarus locations within {hours} hours',
        mapbox_style='dark', mapbox_accesstoken=mapbox_token,
        geo_scope='europe',
    )

    fig.update_layout(margin={"r":0,"t":50,"l":0,"b":0})

    fig.show()