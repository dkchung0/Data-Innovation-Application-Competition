import numpy as np
import pandas as pd

from time import process_time
import time

import arrow
import datetime 
import pymysql

import sys
import os
import re 

import requests
from bs4 import BeautifulSoup 
import urllib.request
import json

def get_fileapi_form_cwb(data_id):
    CWB_URL = 'https://opendata.cwb.gov.tw/fileapi/v1/opendataapi/'
    DATA_ID = data_id
    AUTH_KEY = 'CWB-E0A3AE3C-93F6-443C-BAFA-CDA164DEDAFD'
    FORMAT = 'JSON'
    CWB_API = '{}{}?Authorization={}&format={}'.format(CWB_URL, DATA_ID, AUTH_KEY, FORMAT)
    r = requests.get(CWB_API)
    data = r.json()
    return data

db_settings = {
    "host": "35.194.177.50",
    "port": 3306,
    "user": "root",
    "password": "kimo8971",
    "db": "data_contest",
    "charset": "utf8"
}

data = get_fileapi_form_cwb("F-A0012-001")
n = len(data['cwbopendata']['dataset']['location'])


try:
    conn = pymysql.connect(**db_settings)

    with conn.cursor() as cursor:

        sql = """CREATE TABLE IF NOT EXISTS Sea_weather (
            id int(40) NOT NULL auto_increment UNIQUE,
            Observation_site VARCHAR(20) NOT NULL, 
            Instant_date VARCHAR(20) NOT NULL,
            Lon Float(20) NOT NULL, 
            Lat Float(20) NOT NULL,
            Wave_conditions VARCHAR(20) NOT NULL, 
            Weather_phenomenon VARCHAR(20) NOT NULL,
            PRIMARY KEY (Observation_site, Instant_date))"""

        cursor.execute(sql)

        mySql_insert_query = """INSERT INTO Sea_weather (Observation_site,Instant_date,Lon,Lat,Wave_conditions,Weather_phenomenon)
                                VALUES (%s, %s, %s, %s ,%s ,%s) """

        dl = pd.read_csv("sea_lat_lng.csv",index_col=0)
        
        df=[(data['cwbopendata']['dataset']['location'][i]['locationName'],
            arrow.get(data['cwbopendata']['dataset']['location'][0]['weatherElement'][0]['time'][0]['startTime']).format('YYYY/MM/DD'),
            list(dl.iloc[:,2])[i],
            list(dl.iloc[:,1])[i],
            data['cwbopendata']['dataset']['location'][i]['weatherElement'][4]['time'][0]['parameter']['parameterName'],
            data['cwbopendata']['dataset']['location'][i]['weatherElement'][0]['time'][0]['parameter']['parameterName']) for i in range(n)]

        for i in df:
            try:
                cursor.execute(mySql_insert_query,i)
            except:
                continue

        conn.commit()       

        print("{}_data in {} insert success".format("sea",arrow.now().format('YYYY/MM/DD HH:mm:ss')))

        cursor.close()

except Exception as ex:
    print(ex)