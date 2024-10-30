"""
NHL case study
Benoit Cambournac

app.py
model for logo identified scatter plot
"""
import plotly.graph_objects as go
import plotly.express as px
from app import nhl_snowflake
from PIL import Image
from io import BytesIO
import requests
import cairosvg


def main_scatter(x_stat=None, y_stat=None):

    if x_stat is None and y_stat is None:
        return go.Figure()
    
    app = nhl_snowflake()
    reg_stats = app.get_reg_season_stats()
    # print(reg_stats)

    fig = px.scatter(reg_stats, x=x_stat, y=y_stat, hover_name='TEAM_ABV')
    fig.add_vline(x=reg_stats[x_stat].mean(), line_dash='dash', line_color='blue')
    fig.add_hline(y=reg_stats[y_stat].mean(), line_dash='dash', line_color='blue')

    x_max = reg_stats[x_stat].max()
    y_max = reg_stats[y_stat].max()

    if x_max > 99:
        x_max *= 0.05
    else: x_max = 0.5

    if y_max > 99:
        y_max *= 0.05
    else: y_max = 0.5

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
                sizex=x_max,
                sizey=y_max,
                sizing='contain',
                xanchor="center",
                yanchor="middle",
            )
    
    fig.update_layout(
        height=800
    )
    
    fig.show()


def get_stat_names():

    app = nhl_snowflake()
    reg_stats = app.get_reg_season_stats()

    # reg_stats = reg_stats.sort_values(by=['TEAM_ABV'])
    # reg_stats['TEAM_ABV'].to_csv('teams.csv')

    return list(reg_stats.columns[4:-1])


def bars_with_icons(stat='GOALS'):

    # get each teams' stats
    app = nhl_snowflake()
    reg_stats = app.get_reg_season_stats()
    # print(reg_stats)

    reg_stats = reg_stats.sort_values(by=[stat], ascending=False).head(7).sort_values(by=[stat])

    # fig = px.scatter(reg_stats, x=x_stat, y=y_stat, hover_name='TEAM_ABV')
    fig = px.bar(reg_stats, y='TEAM_ABV', x=stat, orientation='h', opacity=0.2)

    # add the logos as icons to the end of each bar
    for idx, row in reg_stats.iterrows():
    
        if row['LOGO_URL'] is not None:
            # retrieve the image bytes
            img = requests.get(row['LOGO_URL'])

            # convert the image response to a png if it is not
            if img.headers['Content-Type'] == 'image/svg+xml':
                img = cairosvg.svg2png(img.content)

            # add the image on top of the points
            fig.add_layout_image(
                x=row[stat],
                y=row['TEAM_ABV'],
                source=Image.open(BytesIO(img)),
                xref="x",
                yref="y",
                sizex=18,
                sizey=18,
                sizing='contain',
                xanchor="center",
                yanchor="middle",
            )
    
    fig.update_layout(
        height=600,
        width=1200,
        bargap=0.55,
        title=f'{stat.title()} Leaders'
    )

    fig.update_yaxes(showticklabels=False, title='')
    fig.update_xaxes(title='')

    fig.show()



# bars_with_icons()    
# main_scatter('SHOTS', 'GOALS')
# test_app = nhl_snowflake()
# test_app.scarpe_logos()
get_stat_names()

