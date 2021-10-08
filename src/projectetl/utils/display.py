import plotly.graph_objs as go
import json
from dotenv import load_dotenv
import os
import numpy as np
from criteriaetl.utils.display_func import unlimit_display_option


def plot_interactive_geojson(
        df, geojson_path, z_col, id_col, text_col,
        choroplethmapbox_kwargs={}, cbar_kwargs={}, layout_kwargs={}):
    """
    df : DataFrame
        Has to share the `id_col` column label with with the DataFrame
        which generated the geojson.
    geojson_path : path
        Point to the geojson which will be plotted. Has to share `id_col`
        column label with `df` in one of its `'properties'` field.
    z_col : column label
        Indicates column to use for colorating the map
    id_col : column label
        Indicates the column used for matching `df` with `geojson_path`.
    text_col : column label
        Indicated the column to be used as title of the hovertemplate.

    Note
    ----
    This function requires user to have a mapbox token path as
    environment variable (`MAPBOX_TOKEN_PATH`) defined in the project
    `.env` file. To get a token, sign in at: https://docs.mapbox.com/
    """
    # set 'id' field on geojson
    with open(geojson_path) as geofile:
        jdataNo = json.load(geofile)

    for k in range(len(jdataNo['features'])):
        geo_id_ = jdataNo['features'][k]['properties'][id_col]
        jdataNo['features'][k]['id'] = geo_id_

    # get mapbox token
    load_dotenv()
    mapboxt = open(os.getenv('MAPBOX_TOKEN_PATH')).read().rstrip()

    # build figure
    fig = go.Figure(go.Choroplethmapbox(
        z=df[z_col],
        locations=df[id_col],
        geojson=jdataNo,
        text=df[text_col],
        colorscale='Viridis', colorbar=dict(
            thickness=20, ticklen=3, **cbar_kwargs),
        marker_line_width=0.1, marker_opacity=0.5,
        **choroplethmapbox_kwargs))
    fig.update_layout(
        title_x=0.5, width=np.inf, height=900, mapbox=dict(
            center=dict(lat=18.7, lon=-70.145), accesstoken=mapboxt, zoom=7.6
        ), showlegend=False, **layout_kwargs
    )
    return fig


def cell_display(*args):
    """
    Wrapper for display function. It displays  all cell content of
    DataFrames present in `*args`
    """
    return unlimit_display_option('display.max_colwidth', *args)
