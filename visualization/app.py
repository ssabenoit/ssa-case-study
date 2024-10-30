"""
NHL case study
Benoit Cambournac

app.py
manage snowflake connection
"""
import snowflake.connector
import numpy
import pandas as pd
import requests
import cairosvg
from PIL import Image
from io import BytesIO


class nhl_snowflake():

    def __init__(self):
        # create the connection
        self.conn = snowflake.connector.connect(
            user='ssabenoit',
            password='Ben_032603',
            account='jp55454.us-east-2.aws',
            warehouse='DBT_WH',
            database='DBT_ANALYTICS',
            schema='PROD'
        )

    def get_reg_season_stats(self, type='regular', season='20242025'):

        # create a cursor and run the query
        cur = self.conn.cursor()
        if type == 'regular':
            query = f'select * from TEAM_SEASON_STATS_REGULAR where season = {season}'
        cur.execute(query)
        
        description = cur.description
        # pull and format the results into a df
        df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in description])

        for column in description:
            
            # reformat 'Decimal' Columns to floats
            if column[-2] is not None:
                if column[-2] > 0:
                    df[column[0]] = df[column[0]].astype(float)
                    # df[column[0]] = df[column[0]].apply(lambda x: float(x))
            
        # for i in [0, 1, -1]:
        #     df[df.columns[i]] = df[df.columns[i]].astype(str)
        
        # print(df.dtypes)
        cur.close()
        return df
    
    def scarpe_logos(self):

        cur = self.conn.cursor()
        cur.execute("select TEAM_ABV, LOGO_URL from TEAM_SEASON_STATS_REGULAR")
        df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])

        for idx, team in df.iterrows():
            
            if team['LOGO_URL'] is not None:
                # retrieve the image bytes
                img = requests.get(team['LOGO_URL'])

                # convert the image response to a png if it is not
                if img.headers['Content-Type'] == 'image/svg+xml':
                    png_bytes = cairosvg.svg2png(img.content)
                    image = Image.open(BytesIO(png_bytes))

                image.save(f"logos/{team['TEAM_ABV']}.png")
                # with open(f"logos/{team['TEAM_ABV']}.png", "wb") as file:
                #     file.write(image)
        
        cur.close()

    def get_stat_leaders(self, stat, season):
        
        # get all the teams ranked by the given stat
        cur = self.conn.cursor()
        cur.execute(f"select TEAM_ABV, {stat} from TEAM_SEASON_STATS_REGULAR where season = {season} order by {stat} desc")
        df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])

        return df


    def __del__(self):
        # close the snowflake connection
        self.conn.close()
        print('Connection Closed')    


# test_app = nhl_snowflake()
# print(test_app.get_reg_season_stats(season='20242025'))
# test_app.scarpe_logos()
