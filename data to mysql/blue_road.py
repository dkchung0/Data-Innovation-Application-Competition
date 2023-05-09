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

# 基隆馬祖航線

# 臺中馬公航線

# 高雄馬公航線

# 東港小琉球航線

# 臺東綠島航線

# 布袋馬公航線

# 烏石龜山島航線

# 臺中平潭航線

# 臺東蘭嶼航線

# 後壁湖蘭嶼航線

# 基隆龍洞航線

# 龍洞龜山島航線

# 基隆臺中航線

# 基隆台州航線

# 臺中高雄航線

# 臺中金門航線

# 高雄花蓮航線

# 花蓮基隆航線

# 花蓮蘇澳航線

# 臺北港平潭航線

# 安平東吉島航線

# 蘇澳石垣島航線

# 基隆彭佳嶼航線

num = ["01","02","03","04","05","06","07","08","09","10","11","12","13","14",
       "15","16","17","18","19","20","24","25","26"]

def insert_data(code):

    data = get_fileapi_form_cwb("F-A0037-0{}.json".format(code))
    n = len(data["cwbopendata"]['dataset']["location"])

    try:
        conn = pymysql.connect(**db_settings)

        with conn.cursor() as cursor:

            sql = """CREATE TABLE IF NOT EXISTS Blue_road_{} (
                  id int(40) NOT NULL auto_increment UNIQUE,
                  location_code VARCHAR(20) NOT NULL, 
                  Instant_time Datetime NOT NULL, 
                  Wave_height Float(10) NOT NULL, 
                  Wind_speed VARCHAR(20) ,
                  Flow_rate VARCHAR(20),
                  PRIMARY KEY (location_code, Instant_time))""".format(code)
                
            cursor.execute(sql)

            mySql_insert_query = """INSERT INTO Blue_road_{} (location_code,Instant_time,Wave_height,Wind_speed,Flow_rate)
                                    VALUES (%s, %s, %s, %s, %s) """.format(code)

            df=[(data["cwbopendata"]['dataset']["location"][i]["locationCode"],
                 arrow.get(data["cwbopendata"]['dataset']["location"][i]["time"]['dataTime']).datetime,
                 data["cwbopendata"]['dataset']["location"][i]['weatherElement'][0]['elementValue']['value'],
                 data["cwbopendata"]['dataset']["location"][i]['weatherElement'][5]['elementValue']['value'],
                 data["cwbopendata"]['dataset']["location"][i]['weatherElement'][7]['elementValue']['value']) for i in range(n)]

            for i in df:
                try:
                    cursor.execute(mySql_insert_query,i)
                except:
                    continue

            conn.commit()

            print("F-A0037-0{}_data in {} insert success".format(code,arrow.now().format('YYYY/MM/DD HH:mm:ss')))

            cursor.close()

    except Exception as ex:
        print(ex)




if __name__ == "__main__":
    
    for code in num:
        insert_data(code)
