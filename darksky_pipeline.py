import sqlite3
import pandas as pd 
import requests
import numpy as np 


def create_basetable(database, season): 
    
    con = sqlite3.connect(database)

    df_new = pd.read_sql_query(f"""
                SELECT  DISTINCT 
                        m.Match_ID, 
                        m.Season, 
                        m.Date, 
                        m.HomeTeam, 
                        m.AwayTeam, 
                        m.FTHG, 
                        m.FTAG,
                        CASE WHEN m.FTHG > FTAG THEN 1 
                            ELSE 0 
                            END AS WinHome,
                        CASE WHEN m.FTHG <= FTAG THEN 1 
                            ELSE 0 
                            END AS NoWinHome,
                        CASE WHEN m.FTHG < FTAG THEN 1 
                            ELSE 0 
                            END AS WinAway,
                        CASE WHEN m.FTHG >= FTAG THEN 1 
                            ELSE 0 
                            END AS NoWinAway,
                        CASE WHEN m.Div == 'E0' THEN 'English Premier League' 
                            ELSE 'Bundesliga' 
                            END as League

                FROM Matches AS m
                WHERE m.season = {season}
                ORDER BY m.Match_ID 
                """, con)
    return df_new


def add_apis (df_new):
    with open('.secrets') as f:
        password=f.read().strip()
    
    df_new['New_Date'] = df_new['Date']+'T00:00:00' 
    dates =df_new['New_Date'].unique()
    
    pts = [] 
    for date in dates[0:2]:
        url = f'https://api.darksky.net/forecast/{password}/52.52,13.405,{date}?exclude=minutely,hourly, alerts, flags'   
        response = requests.get(url)
        if response.status_code == 200:
            response_dict = response.json()
            pts.append(response_dict['daily']['data'][0]['precipIntensity'])
        else:
            print("Fail to get response")
    
    date_rain = dict(zip(dates, pts))
    date_rain = list(date_rain.items())
    date_rain = pd.DataFrame(date_rain, columns = ['New_Date', 'precipIntensity'])
    
    df_new = df_new.merge(date_rain,how='left',on='New_Date')
    
    return df_new

def create_table(df_new):
    df_new['precipIntensity'][df_new['precipIntensity'] >0]= 1 
    df_new['precipIntensity'][df_new['precipIntensity'] == 0]= 0
    
    df_home = df_new[['HomeTeam', 
                      'League', 
                      'Season', 
                      'FTHG',
                      'WinHome', 
                      'NoWinHome',
                      'precipIntensity']]
    df_home.rename(columns={'HomeTeam':'TeamName', 
                            'FTHG': 'Goals', 
                            'WinHome': 'Wins', 
                            'NoWinHome': 'NoWin'}, 
                            inplace =True)
    
    df_away = df_new[['AwayTeam', 
                      'League', 
                      'Season', 
                      'FTAG',
                      'WinAway',
                      'NoWinAway',
                      'precipIntensity']]
    df_away.rename(columns={'AwayTeam':'TeamName', 
                            'FTAG': 'Goals', 
                            'WinAway': 'Wins', 
                            'NoWinAway': 'NoWin'}, 
                            inplace=True)
    
    df_com = pd.concat([df_home, df_away], ignore_index=False)
    
    df_com['TotalGame'] = df_com['Wins'] + df_com['NoWin']
    
    df_com.loc[(df_com['Wins'] ==1)& (df_com['precipIntensity'] ==1), 'WinRainday'] =1 
    
    df_agg = df_com.groupby(['TeamName', 
                             'League', 
                             'Season']).agg({'precipIntensity':'sum',
                                             'Goals':'sum', 
                                             'Wins':'sum', 
                                             'TotalGame':'sum',
                                             'WinRainday': 'sum'})
    
    df_agg['WinPercentRainDay']=(df_agg['WinRainday']/df_agg['precipIntensity'])
    df_agg = df_agg.reset_index()
    df_agg = df_agg[['TeamName',
                     'League', 
                     'Season', 
                     'Goals', 
                     'Wins', 
                     'WinPercentRainDay']]

    return df_agg


