"""
NHL case study
Benoit Cambournac

dash.py
dash application to run a simple version of logo nhl scatter plot
"""
from dash import Dash, dcc, html, Input, Output, State
import pandas as pd
from scatter import get_stat_names # main_scatter, 
from app import nhl_snowflake
from PIL import Image
from io import BytesIO
import requests
import cairosvg
import plotly.express as px


app = Dash(__name__)
app.layout = html.Div([

    html.H1("NHL Team Stats Comparison"),

    # html.Div([
        # drop downs for x and y axis stats for scatter display
    dcc.Dropdown(get_stat_names(), id='x_axis_dropdown', style={'display': 'inline-block', 'width': '50%'}),
    dcc.Dropdown(get_stat_names(), id='y_axis_dropdown', style={'display': 'inline-block', 'width': '50%'}),
    # ]),

    dcc.Graph(id='scatter_plot')

])


@app.callback(
    Output('scatter_plot', 'figure'),
    Input('x_axis_dropdown', 'value'),
    Input('y_axis_dropdown', 'value'), 
    prevent_initial_call=True
)
def update_scatter(x_stat, y_stat):

    if x_stat is not None and y_stat is not None:
        # fig = main_scatter(x_axis, y_axis)
        app = nhl_snowflake()
        reg_stats = app.get_reg_season_stats()
        print('upd')

        fig = px.scatter(reg_stats, x=x_stat, y=y_stat, hover_name='TEAM_ABV')
        fig.add_vline(x=reg_stats[x_stat].mean(), line_dash='dash', line_color='blue')
        fig.add_hline(y=reg_stats[y_stat].mean(), line_dash='dash', line_color='blue')

        for idx, row in reg_stats.iterrows():
        
            if row['LOGO_URL'] is not None:
                # retrieve the image bytes
                img = requests.get(row['LOGO_URL'])

                # convert the image response to a png if it is not
                if img.headers['Content-Type'] == 'image/svg+xml':
                    img = cairosvg.svg2png(img.content)

                # add the image on top of the points
                fig.add_layout_image(
                    x=row[x_stat],
                    y=row[y_stat],
                    source=Image.open(BytesIO(img)),
                    xref="x",
                    yref="y",
                    sizex=0.5,
                    sizey=0.5,
                    xanchor="center",
                    yanchor="middle",
                )
        
        return fig


app.run_server(debug=True)