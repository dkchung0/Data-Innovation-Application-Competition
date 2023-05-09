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

#########################################################################

# 海水浴場(三天3小時)

data = get_fileapi_form_cwb("F-B0053-005.json")

try:
    conn = pymysql.connect(**db_settings)

    with conn.cursor() as cursor:

        sql ="""CREATE TABLE IF NOT EXISTS Beach_three_day (
             id int(40) NOT NULL auto_increment UNIQUE,
             Observation_site VARCHAR(20) NOT NULL, 
             Instant_time Datetime NOT NULL, 
             Lon Float(10) NOT NULL, 
             Lat Float(10) NOT NULL,
             temperature VARCHAR(20) NOT NULL ,
             body_temperature VARCHAR(20) NOT NULL ,
             relative_humidity VARCHAR(20) NOT NULL ,
             Chance_of_rain  VARCHAR(20) NOT NULL ,
             Maximum_wind_speed  VARCHAR(20) NOT NULL ,
             Weather_phenomenon VARCHAR(20) NOT NULL ,
             Maximum_comfort_index VARCHAR(20) NOT NULL,
             PRIMARY KEY (Observation_site, Instant_time))"""
        cursor.execute(sql)

        mySql_insert_query = """INSERT INTO Beach_three_day (Observation_site,Instant_time,Lon,Lat,temperature,body_temperature,relative_humidity,Chance_of_rain,Maximum_wind_speed,Weather_phenomenon,Maximum_comfort_index) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """

        df = pd.DataFrame([])

        for i in range(len(data['cwbopendata']['dataset']['locations']['location'])):
            for j in range(len(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'])):
                try:
                    df = df.append({"地點":data['cwbopendata']['dataset']['locations']['location'][i]['locationName'],
                                    "時間":arrow.get(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j]['dataTime']).format("YYYY-MM-DD HH:mm:SS"),
                                    "經度":data['cwbopendata']['dataset']['locations']['location'][i]['lon'],
                                    "緯度":data['cwbopendata']['dataset']['locations']['location'][i]['lat'],
                                    "溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j]['elementValue']['value']+"°C",
                                    "相對濕度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][2]['time'][j]['elementValue']['value']+"%",
                                    "降雨機率":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][3]['time'][j//2]['elementValue']['value'],
                                    "風向":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][5]['time'][j]['elementValue']['value'],
                                    "最大風速":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][6]['time'][j]['elementValue'][0]['value'],
                                    "舒適度指數":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][7]['time'][j]['elementValue'][1]['value'],
                                    "體感溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][8]['time'][j]['elementValue']['value']+"°C",
                                    "天氣現象":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][9]['time'][j]['elementValue'][0]['value']},ignore_index=True)
                except:
                    df = df.append({"地點":data['cwbopendata']['dataset']['locations']['location'][i]['locationName'],
                                    "時間":arrow.get(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j]['dataTime']).format("YYYY-MM-DD HH:mm:SS"),
                                    "經度":data['cwbopendata']['dataset']['locations']['location'][i]['lon'],
                                    "緯度":data['cwbopendata']['dataset']['locations']['location'][i]['lat'],
                                    "溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j]['elementValue']['value']+"°C",
                                    "相對濕度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][2]['time'][j]['elementValue']['value']+"%",
                                    "降雨機率":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][3]['time'][(j//2)-1]['elementValue']['value'],
                                    "風向":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][5]['time'][j]['elementValue']['value'],
                                    "最大風速":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][6]['time'][j]['elementValue'][0]['value'],
                                    "舒適度指數":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][7]['time'][j]['elementValue'][1]['value'],
                                    "體感溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][8]['time'][j]['elementValue']['value']+"°C",
                                    "天氣現象":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][9]['time'][j]['elementValue'][0]['value']},ignore_index=True)
        
        df.降雨機率 = df.降雨機率.apply(lambda x : "0%" if x==None else str(x)+"%" )
        m = len(df)

        wdf = [(df.iloc[i,0],df.iloc[i,1],df.iloc[i,2],df.iloc[i,3],df.iloc[i,4],df.iloc[i,10],df.iloc[i,5],df.iloc[i,6],df.iloc[i,8],df.iloc[i,11],df.iloc[i,9]) for i in range(m)]

        for i in wdf:
            try:
                cursor.execute(mySql_insert_query,i)
            except:
                continue


        conn.commit()

        print("{}_data in {} insert success".format("Beach_threeday",arrow.now().format('YYYY/MM/DD HH:mm:ss')))

        cursor.close()

except Exception as ex:
    print(ex)




#########################################################################

# 海釣(三天3小時)

data = get_fileapi_form_cwb("F-B0053-023.json")

try:
    conn = pymysql.connect(**db_settings)

    with conn.cursor() as cursor:

        sql ="""CREATE TABLE IF NOT EXISTS Fishing_three_day (
             id int(40) NOT NULL auto_increment UNIQUE,
             Observation_site VARCHAR(20) NOT NULL, 
             Instant_time Datetime NOT NULL, 
             Lon Float(10) NOT NULL, 
             Lat Float(10) NOT NULL,
             temperature VARCHAR(20) NOT NULL ,
             body_temperature VARCHAR(20) NOT NULL ,
             relative_humidity VARCHAR(20) NOT NULL ,
             Chance_of_rain  VARCHAR(20) NOT NULL ,
             Maximum_wind_speed  VARCHAR(20) NOT NULL ,
             Weather_phenomenon VARCHAR(20) NOT NULL ,
             Maximum_comfort_index VARCHAR(20) NOT NULL,
             PRIMARY KEY (Observation_site, Instant_time))"""
        cursor.execute(sql)

        mySql_insert_query = """INSERT INTO Fishing_three_day (Observation_site,Instant_time,Lon,Lat,temperature,body_temperature,relative_humidity,Chance_of_rain,Maximum_wind_speed,Weather_phenomenon,Maximum_comfort_index) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """

        df = pd.DataFrame([])

        for i in range(len(data['cwbopendata']['dataset']['locations']['location'])):
            for j in range(len(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'])):
                try:
                    df = df.append({"地點":data['cwbopendata']['dataset']['locations']['location'][i]['locationName'],
                                    "時間":arrow.get(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j]['dataTime']).format("YYYY-MM-DD HH:mm:SS"),
                                    "經度":data['cwbopendata']['dataset']['locations']['location'][i]['lon'],
                                    "緯度":data['cwbopendata']['dataset']['locations']['location'][i]['lat'],
                                    "溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j]['elementValue']['value']+"°C",
                                    "相對濕度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][2]['time'][j]['elementValue']['value']+"%",
                                    "降雨機率":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][3]['time'][j//2]['elementValue']['value'],
                                    "風向":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][5]['time'][j]['elementValue']['value'],
                                    "最大風速":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][6]['time'][j]['elementValue'][0]['value'],
                                    "舒適度指數":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][7]['time'][j]['elementValue'][1]['value'],
                                    "體感溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][8]['time'][j]['elementValue']['value']+"°C",
                                    "天氣現象":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][9]['time'][j]['elementValue'][0]['value']},ignore_index=True)
                except:
                    df = df.append({"地點":data['cwbopendata']['dataset']['locations']['location'][i]['locationName'],
                                    "時間":arrow.get(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j]['dataTime']).format("YYYY-MM-DD HH:mm:SS"),
                                    "經度":data['cwbopendata']['dataset']['locations']['location'][i]['lon'],
                                    "緯度":data['cwbopendata']['dataset']['locations']['location'][i]['lat'],
                                    "溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j]['elementValue']['value']+"°C",
                                    "相對濕度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][2]['time'][j]['elementValue']['value']+"%",
                                    "降雨機率":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][3]['time'][(j//2)-1]['elementValue']['value'],
                                    "風向":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][5]['time'][j]['elementValue']['value'],
                                    "最大風速":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][6]['time'][j]['elementValue'][0]['value'],
                                    "舒適度指數":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][7]['time'][j]['elementValue'][1]['value'],
                                    "體感溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][8]['time'][j]['elementValue']['value']+"°C",
                                    "天氣現象":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][9]['time'][j]['elementValue'][0]['value']},ignore_index=True)
        
        df.降雨機率 = df.降雨機率.apply(lambda x : "0%" if x==None else str(x)+"%" )
        m = len(df)

        wdf = [(df.iloc[i,0],df.iloc[i,1],df.iloc[i,2],df.iloc[i,3],df.iloc[i,4],df.iloc[i,10],df.iloc[i,5],df.iloc[i,6],df.iloc[i,8],df.iloc[i,11],df.iloc[i,9]) for i in range(m)]

        for i in wdf:
            try:
                cursor.execute(mySql_insert_query,i)
            except:
                continue

        conn.commit()

        print("{}_data in {} insert success".format("Fishing_threeday",arrow.now().format('YYYY/MM/DD HH:mm:ss')))

        cursor.close()

except Exception as ex:
    print(ex)




#########################################################################

# 娛樂漁業(三天3小時)

data = get_fileapi_form_cwb("F-B0053-029.json")

try:
    conn = pymysql.connect(**db_settings)

    with conn.cursor() as cursor:

        sql ="""CREATE TABLE IF NOT EXISTS Entertainment_three_day (
             id int(40) NOT NULL auto_increment UNIQUE,
             Observation_site VARCHAR(20) NOT NULL, 
             Instant_time Datetime NOT NULL, 
             Lon Float(10) NOT NULL, 
             Lat Float(10) NOT NULL,
             temperature VARCHAR(20) NOT NULL ,
             body_temperature VARCHAR(20) NOT NULL ,
             relative_humidity VARCHAR(20) NOT NULL ,
             Chance_of_rain  VARCHAR(20) NOT NULL ,
             Maximum_wind_speed  VARCHAR(20) NOT NULL ,
             Weather_phenomenon VARCHAR(20) NOT NULL ,
             Maximum_comfort_index VARCHAR(20) NOT NULL,
             PRIMARY KEY (Observation_site, Instant_time))"""
        cursor.execute(sql)

        mySql_insert_query = """INSERT INTO Entertainment_three_day (Observation_site,Instant_time,Lon,Lat,temperature,body_temperature,relative_humidity,Chance_of_rain,Maximum_wind_speed,Weather_phenomenon,Maximum_comfort_index) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """

        df = pd.DataFrame([])

        for i in range(len(data['cwbopendata']['dataset']['locations']['location'])):
            for j in range(len(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'])):
                try:
                    df = df.append({"地點":data['cwbopendata']['dataset']['locations']['location'][i]['locationName'],
                                    "時間":arrow.get(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j]['dataTime']).format("YYYY-MM-DD HH:mm:SS"),
                                    "經度":data['cwbopendata']['dataset']['locations']['location'][i]['lon'],
                                    "緯度":data['cwbopendata']['dataset']['locations']['location'][i]['lat'],
                                    "溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j]['elementValue']['value']+"°C",
                                    "相對濕度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][2]['time'][j]['elementValue']['value']+"%",
                                    "降雨機率":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][3]['time'][j//2]['elementValue']['value'],
                                    "風向":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][5]['time'][j]['elementValue']['value'],
                                    "最大風速":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][6]['time'][j]['elementValue'][0]['value'],
                                    "舒適度指數":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][7]['time'][j]['elementValue'][1]['value'],
                                    "體感溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][8]['time'][j]['elementValue']['value']+"°C",
                                    "天氣現象":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][9]['time'][j]['elementValue'][0]['value']},ignore_index=True)
                except:
                    df = df.append({"地點":data['cwbopendata']['dataset']['locations']['location'][i]['locationName'],
                                    "時間":arrow.get(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j]['dataTime']).format("YYYY-MM-DD HH:mm:SS"),
                                    "經度":data['cwbopendata']['dataset']['locations']['location'][i]['lon'],
                                    "緯度":data['cwbopendata']['dataset']['locations']['location'][i]['lat'],
                                    "溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j]['elementValue']['value']+"°C",
                                    "相對濕度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][2]['time'][j]['elementValue']['value']+"%",
                                    "降雨機率":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][3]['time'][(j//2)-1]['elementValue']['value'],
                                    "風向":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][5]['time'][j]['elementValue']['value'],
                                    "最大風速":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][6]['time'][j]['elementValue'][0]['value'],
                                    "舒適度指數":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][7]['time'][j]['elementValue'][1]['value'],
                                    "體感溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][8]['time'][j]['elementValue']['value']+"°C",
                                    "天氣現象":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][9]['time'][j]['elementValue'][0]['value']},ignore_index=True)
        
        df.降雨機率 = df.降雨機率.apply(lambda x : "0%" if x==None else str(x)+"%" )
        m = len(df)

        wdf = [(df.iloc[i,0],df.iloc[i,1],df.iloc[i,2],df.iloc[i,3],df.iloc[i,4],df.iloc[i,10],df.iloc[i,5],df.iloc[i,6],df.iloc[i,8],df.iloc[i,11],df.iloc[i,9]) for i in range(m)]

        for i in wdf:
            try:
                cursor.execute(mySql_insert_query,i)
            except:
                continue

        conn.commit()

        print("{}_data in {} insert success".format("Entertainment_threeday",arrow.now().format('YYYY/MM/DD HH:mm:ss')))
        
        cursor.close()

except Exception as ex:
    print(ex)




#########################################################################

# 港口(三天3小時)

data = get_fileapi_form_cwb("F-B0053-053.json")

try:
    conn = pymysql.connect(**db_settings)

    with conn.cursor() as cursor:

        sql ="""CREATE TABLE IF NOT EXISTS Port_three_day (
             id int(40) NOT NULL auto_increment UNIQUE,
             Observation_site VARCHAR(20) NOT NULL, 
             Instant_time Datetime NOT NULL, 
             Lon Float(10) NOT NULL, 
             Lat Float(10) NOT NULL,
             temperature VARCHAR(20) NOT NULL ,
             body_temperature VARCHAR(20) NOT NULL ,
             relative_humidity VARCHAR(20) NOT NULL ,
             Chance_of_rain  VARCHAR(20) NOT NULL ,
             Maximum_wind_speed  VARCHAR(20) NOT NULL ,
             Weather_phenomenon VARCHAR(20) NOT NULL ,
             Maximum_comfort_index VARCHAR(20) NOT NULL,
             PRIMARY KEY (Observation_site, Instant_time))"""
        cursor.execute(sql)

        mySql_insert_query = """INSERT INTO Port_three_day (Observation_site,Instant_time,Lon,Lat,temperature,body_temperature,relative_humidity,Chance_of_rain,Maximum_wind_speed,Weather_phenomenon,Maximum_comfort_index) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """

        df = pd.DataFrame([])

        for i in range(len(data['cwbopendata']['dataset']['locations']['location'])):
            for j in range(len(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'])):
                try:
                    df = df.append({"地點":data['cwbopendata']['dataset']['locations']['location'][i]['locationName'],
                                    "時間":arrow.get(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j]['dataTime']).format("YYYY-MM-DD HH:mm:SS"),
                                    "經度":data['cwbopendata']['dataset']['locations']['location'][i]['lon'],
                                    "緯度":data['cwbopendata']['dataset']['locations']['location'][i]['lat'],
                                    "溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j]['elementValue']['value']+"°C",
                                    "相對濕度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][2]['time'][j]['elementValue']['value']+"%",
                                    "降雨機率":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][3]['time'][j//2]['elementValue']['value'],
                                    "風向":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][5]['time'][j]['elementValue']['value'],
                                    "最大風速":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][6]['time'][j]['elementValue'][0]['value'],
                                    "舒適度指數":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][7]['time'][j]['elementValue'][1]['value'],
                                    "體感溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][8]['time'][j]['elementValue']['value']+"°C",
                                    "天氣現象":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][9]['time'][j]['elementValue'][0]['value']},ignore_index=True)
                except:
                    df = df.append({"地點":data['cwbopendata']['dataset']['locations']['location'][i]['locationName'],
                                    "時間":arrow.get(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j]['dataTime']).format("YYYY-MM-DD HH:mm:SS"),
                                    "經度":data['cwbopendata']['dataset']['locations']['location'][i]['lon'],
                                    "緯度":data['cwbopendata']['dataset']['locations']['location'][i]['lat'],
                                    "溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j]['elementValue']['value']+"°C",
                                    "相對濕度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][2]['time'][j]['elementValue']['value']+"%",
                                    "降雨機率":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][3]['time'][(j//2)-1]['elementValue']['value'],
                                    "風向":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][5]['time'][j]['elementValue']['value'],
                                    "最大風速":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][6]['time'][j]['elementValue'][0]['value'],
                                    "舒適度指數":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][7]['time'][j]['elementValue'][1]['value'],
                                    "體感溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][8]['time'][j]['elementValue']['value']+"°C",
                                    "天氣現象":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][9]['time'][j]['elementValue'][0]['value']},ignore_index=True)
        
        df.降雨機率 = df.降雨機率.apply(lambda x : "0%" if x==None else str(x)+"%" )
        m = len(df)

        wdf = [(df.iloc[i,0],df.iloc[i,1],df.iloc[i,2],df.iloc[i,3],df.iloc[i,4],df.iloc[i,10],df.iloc[i,5],df.iloc[i,6],df.iloc[i,8],df.iloc[i,11],df.iloc[i,9]) for i in range(m)]

        for i in wdf:
            try:
                cursor.execute(mySql_insert_query,i)
            except:
                continue

        conn.commit()

        print("{}_data in {} insert success".format("Port_threeday",arrow.now().format('YYYY/MM/DD HH:mm:ss')))

        cursor.close()

except Exception as ex:
    print(ex)


#########################################################################

# 浮潛(三天3小時)

data = get_fileapi_form_cwb("F-B0053-078.json")

try:
    conn = pymysql.connect(**db_settings)

    with conn.cursor() as cursor:

        sql ="""CREATE TABLE IF NOT EXISTS Snorkeling_three_day (
             id int(40) NOT NULL auto_increment UNIQUE,
             Observation_site VARCHAR(20) NOT NULL, 
             Instant_time Datetime NOT NULL, 
             Lon Float(10) NOT NULL, 
             Lat Float(10) NOT NULL,
             temperature VARCHAR(20) NOT NULL ,
             body_temperature VARCHAR(20) NOT NULL ,
             relative_humidity VARCHAR(20) NOT NULL ,
             Chance_of_rain  VARCHAR(20) NOT NULL ,
             Maximum_wind_speed  VARCHAR(20) NOT NULL ,
             Weather_phenomenon VARCHAR(20) NOT NULL ,
             Maximum_comfort_index VARCHAR(20) NOT NULL,
             PRIMARY KEY (Observation_site, Instant_time))"""
        cursor.execute(sql)

        mySql_insert_query = """INSERT INTO Snorkeling_three_day (Observation_site,Instant_time,Lon,Lat,temperature,body_temperature,relative_humidity,Chance_of_rain,Maximum_wind_speed,Weather_phenomenon,Maximum_comfort_index) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """

        df = pd.DataFrame([])

        for i in range(len(data['cwbopendata']['dataset']['locations']['location'])):
            for j in range(len(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'])):
                try:
                    df = df.append({"地點":data['cwbopendata']['dataset']['locations']['location'][i]['locationName'],
                                    "時間":arrow.get(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j]['dataTime']).format("YYYY-MM-DD HH:mm:SS"),
                                    "經度":data['cwbopendata']['dataset']['locations']['location'][i]['lon'],
                                    "緯度":data['cwbopendata']['dataset']['locations']['location'][i]['lat'],
                                    "溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j]['elementValue']['value']+"°C",
                                    "相對濕度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][2]['time'][j]['elementValue']['value']+"%",
                                    "降雨機率":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][3]['time'][j//2]['elementValue']['value'],
                                    "風向":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][5]['time'][j]['elementValue']['value'],
                                    "最大風速":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][6]['time'][j]['elementValue'][0]['value'],
                                    "舒適度指數":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][7]['time'][j]['elementValue'][1]['value'],
                                    "體感溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][8]['time'][j]['elementValue']['value']+"°C",
                                    "天氣現象":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][9]['time'][j]['elementValue'][0]['value']},ignore_index=True)
                except:
                    df = df.append({"地點":data['cwbopendata']['dataset']['locations']['location'][i]['locationName'],
                                    "時間":arrow.get(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j]['dataTime']).format("YYYY-MM-DD HH:mm:SS"),
                                    "經度":data['cwbopendata']['dataset']['locations']['location'][i]['lon'],
                                    "緯度":data['cwbopendata']['dataset']['locations']['location'][i]['lat'],
                                    "溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j]['elementValue']['value']+"°C",
                                    "相對濕度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][2]['time'][j]['elementValue']['value']+"%",
                                    "降雨機率":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][3]['time'][(j//2)-1]['elementValue']['value'],
                                    "風向":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][5]['time'][j]['elementValue']['value'],
                                    "最大風速":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][6]['time'][j]['elementValue'][0]['value'],
                                    "舒適度指數":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][7]['time'][j]['elementValue'][1]['value'],
                                    "體感溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][8]['time'][j]['elementValue']['value']+"°C",
                                    "天氣現象":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][9]['time'][j]['elementValue'][0]['value']},ignore_index=True)
        
        df.降雨機率 = df.降雨機率.apply(lambda x : "0%" if x==None else str(x)+"%" )
        m = len(df)

        wdf = [(df.iloc[i,0],df.iloc[i,1],df.iloc[i,2],df.iloc[i,3],df.iloc[i,4],df.iloc[i,10],df.iloc[i,5],df.iloc[i,6],df.iloc[i,8],df.iloc[i,11],df.iloc[i,9]) for i in range(m)]

        for i in wdf:
            try:
                cursor.execute(mySql_insert_query,i)
            except:
                continue

        conn.commit()

        print("{}_data in {} insert success".format("Snorkeling_threeday",arrow.now().format('YYYY/MM/DD HH:mm:ss')))

        cursor.close()

except Exception as ex:
    print(ex)




#########################################################################

# 衝浪(三天3小時)

data = get_fileapi_form_cwb("F-B0053-084.json")

try:
    conn = pymysql.connect(**db_settings)

    with conn.cursor() as cursor:

        sql ="""CREATE TABLE IF NOT EXISTS Surfing_three_day (
             id int(40) NOT NULL auto_increment UNIQUE,
             Observation_site VARCHAR(20) NOT NULL, 
             Instant_time Datetime NOT NULL, 
             Lon Float(10) NOT NULL, 
             Lat Float(10) NOT NULL,
             temperature VARCHAR(20) NOT NULL ,
             body_temperature VARCHAR(20) NOT NULL ,
             relative_humidity VARCHAR(20) NOT NULL ,
             Chance_of_rain  VARCHAR(20) NOT NULL ,
             Maximum_wind_speed  VARCHAR(20) NOT NULL ,
             Weather_phenomenon VARCHAR(20) NOT NULL ,
             Maximum_comfort_index VARCHAR(20) NOT NULL,
             PRIMARY KEY (Observation_site, Instant_time))"""
        cursor.execute(sql)

        mySql_insert_query = """INSERT INTO Surfing_three_day (Observation_site,Instant_time,Lon,Lat,temperature,body_temperature,relative_humidity,Chance_of_rain,Maximum_wind_speed,Weather_phenomenon,Maximum_comfort_index) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """

        df = pd.DataFrame([])

        for i in range(len(data['cwbopendata']['dataset']['locations']['location'])):
            for j in range(len(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'])):
                try:
                    df = df.append({"地點":data['cwbopendata']['dataset']['locations']['location'][i]['locationName'],
                                    "時間":arrow.get(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j]['dataTime']).format("YYYY-MM-DD HH:mm:SS"),
                                    "經度":data['cwbopendata']['dataset']['locations']['location'][i]['lon'],
                                    "緯度":data['cwbopendata']['dataset']['locations']['location'][i]['lat'],
                                    "溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j]['elementValue']['value']+"°C",
                                    "相對濕度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][2]['time'][j]['elementValue']['value']+"%",
                                    "降雨機率":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][3]['time'][j//2]['elementValue']['value'],
                                    "風向":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][5]['time'][j]['elementValue']['value'],
                                    "最大風速":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][6]['time'][j]['elementValue'][0]['value'],
                                    "舒適度指數":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][7]['time'][j]['elementValue'][1]['value'],
                                    "體感溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][8]['time'][j]['elementValue']['value']+"°C",
                                    "天氣現象":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][9]['time'][j]['elementValue'][0]['value']},ignore_index=True)
                except:
                    df = df.append({"地點":data['cwbopendata']['dataset']['locations']['location'][i]['locationName'],
                                    "時間":arrow.get(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j]['dataTime']).format("YYYY-MM-DD HH:mm:SS"),
                                    "經度":data['cwbopendata']['dataset']['locations']['location'][i]['lon'],
                                    "緯度":data['cwbopendata']['dataset']['locations']['location'][i]['lat'],
                                    "溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j]['elementValue']['value']+"°C",
                                    "相對濕度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][2]['time'][j]['elementValue']['value']+"%",
                                    "降雨機率":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][3]['time'][(j//2)-1]['elementValue']['value'],
                                    "風向":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][5]['time'][j]['elementValue']['value'],
                                    "最大風速":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][6]['time'][j]['elementValue'][0]['value'],
                                    "舒適度指數":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][7]['time'][j]['elementValue'][1]['value'],
                                    "體感溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][8]['time'][j]['elementValue']['value']+"°C",
                                    "天氣現象":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][9]['time'][j]['elementValue'][0]['value']},ignore_index=True)
        
        df.降雨機率 = df.降雨機率.apply(lambda x : "0%" if x==None else str(x)+"%" )
        m = len(df)

        wdf = [(df.iloc[i,0],df.iloc[i,1],df.iloc[i,2],df.iloc[i,3],df.iloc[i,4],df.iloc[i,10],df.iloc[i,5],df.iloc[i,6],df.iloc[i,8],df.iloc[i,11],df.iloc[i,9]) for i in range(m)]

        for i in wdf:
            try:
                cursor.execute(mySql_insert_query,i)
            except:
                continue

        conn.commit()

        print("{}_data in {} insert success".format("Surfing_threeday",arrow.now().format('YYYY/MM/DD HH:mm:ss')))

        cursor.close()

except Exception as ex:
    print(ex)