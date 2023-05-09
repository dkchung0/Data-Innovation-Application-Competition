import numpy as np
import pandas as pd

from time import process_time
import time

import arrow
import datetime 
import pymysql

import sys
import os

import requests
from bs4 import BeautifulSoup 
import urllib.request
import json

def get_data(data_id):
    url3 = "https://opendata.cwb.gov.tw/api/v1/rest/datastore/{}?Authorization={}&format={}"
    dataid = data_id
    apikey = "CWB-402E7D68-A5DB-4F96-BB4B-BBE7CD372BE5"
    data_type = 'json'

    link = url3.format(dataid,apikey,data_type)

    urllib.request.urlretrieve(link,'{}.{}'.format(dataid, data_type))

    with open("{}.{}".format(dataid,data_type),"r",encoding="utf-8-sig") as file: 
        data = json.load(file)

    return data

db_settings = {
    "host": "35.194.177.50",
    "port": 3306,
    "user": "root",
    "password": "kimo8971",
    "db": "data_contest",
    "charset": "utf8"
}

#########################################################################

# 自動氣象站-氣象觀測資料

data = get_data('O-A0001-001')
n  = len(data["records"]["location"])


try:
    conn = pymysql.connect(**db_settings)

    with conn.cursor() as cursor:

        sql = """CREATE TABLE IF NOT EXISTS Instant_weather (
              id int(40) NOT NULL auto_increment UNIQUE,
              Observation_site VARCHAR(20) NOT NULL, 
              Instant_time Datetime NOT NULL, 
              City VARCHAR(20) NOT NULL, 
              Township VARCHAR(20) NOT NULL, 
              Lon Float(10) NOT NULL, 
              Lat Float(10) NOT NULL, 
              Wind_speed Float(10) NOT NULL, 
              Temperature Float(10) NOT NULL, 
              Relative_humidity Float(10) , 
              Cumulative_rainfall Float(10) , 
              Air_pressure Float(10) , 
              Minimum_temperature Float(10) , 
              Maximum_temperature Float(10) ,
              PRIMARY KEY (City ,Observation_site, Instant_time))"""

        cursor.execute(sql)

        mySql_insert_query = """INSERT INTO Instant_weather (Observation_site,Instant_time,City,Township,Lon,Lat,Wind_speed,Temperature,
                                                             Relative_humidity,Cumulative_rainfall,Air_pressure,Minimum_temperature,Maximum_temperature) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """

        df=[(data["records"]["location"][i]["locationName"],
               data["records"]["location"][i]['time']['obsTime'],
               data["records"]["location"][i]["parameter"][0]["parameterValue"],
               data["records"]["location"][i]["parameter"][2]["parameterValue"],
               data["records"]["location"][i]["lon"],
               data["records"]["location"][i]["lat"],
               data["records"]["location"][i]["weatherElement"][2]['elementValue'],
               data["records"]["location"][i]["weatherElement"][3]['elementValue'],
               data["records"]["location"][i]["weatherElement"][4]['elementValue'],
               data["records"]["location"][i]["weatherElement"][6]['elementValue'],
               data["records"]["location"][i]["weatherElement"][5]['elementValue'],
               data["records"]["location"][i]["weatherElement"][12]['elementValue'],
               data["records"]["location"][i]["weatherElement"][10]['elementValue']) for i in range(n)]

        for i in df:
            try:
                cursor.execute(mySql_insert_query,i)
            except:
                continue

        conn.commit()

        print("{}_data in {} insert success".format("instant_observation",arrow.now().format('YYYY/MM/DD HH:mm:ss')))

        cursor.close()


except Exception as ex:
    print(ex)

