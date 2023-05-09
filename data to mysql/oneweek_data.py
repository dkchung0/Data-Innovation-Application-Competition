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

# 海水浴場(一週日夜)

data = get_fileapi_form_cwb("F-B0053-003.json")

try:
    conn = pymysql.connect(**db_settings)

    with conn.cursor() as cursor:

        sql ="""CREATE TABLE IF NOT EXISTS Beach_one_week (
             id int(40) NOT NULL auto_increment UNIQUE,
             Observation_site VARCHAR(20) NOT NULL, 
             Instant_time Datetime NOT NULL, 
             Lon Float(10) NOT NULL, 
             Lat Float(10) NOT NULL,
             Average_temperature VARCHAR(20) NOT NULL ,
             Average_relative_humidity VARCHAR(20) NOT NULL ,
             Minimum_temperature VARCHAR(20) NOT NULL ,
             Maximum_temperature VARCHAR(20) NOT NULL ,
             Chance_of_rain  VARCHAR(20) NOT NULL ,
             Maximum_wind_speed  VARCHAR(20) NOT NULL ,
             Weather_phenomenon VARCHAR(20) NOT NULL ,
             Maximum_comfort_index VARCHAR(20) NOT NULL,
             PRIMARY KEY (Observation_site, Instant_time))"""
        cursor.execute(sql)

        mySql_insert_query = """INSERT INTO Beach_one_week (Observation_site,Instant_time,Lon,Lat,Average_temperature,Average_relative_humidity, Minimum_temperature,
                                                             Maximum_temperature,Chance_of_rain,Maximum_wind_speed,Weather_phenomenon,Maximum_comfort_index) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """

        df = pd.DataFrame([])

        for i in range(len(data['cwbopendata']['dataset']['locations']['location'])):
            for j in range(len(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'])-1):
                df = df.append({"地點":data['cwbopendata']['dataset']['locations']['location'][i]['locationName'],
                                "時間":arrow.get(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j+1]['startTime']).format("YYYY-MM-DD HH:mm:SS"),
                                "經度":data['cwbopendata']['dataset']['locations']['location'][i]['lon'],
                                "緯度":data['cwbopendata']['dataset']['locations']['location'][i]['lat'],
                                "平均溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j+1]['elementValue']['value']+"°C",
                                "平均相對濕度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][2]['time'][j+1]['elementValue']['value']+"%",
                                "最高溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][3]['time'][j+1]['elementValue']['value']+"°C",
                                "最低溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][4]['time'][j+1]['elementValue']['value']+"°C",
                                "最大舒適度指數":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][7]['time'][j+1]['elementValue'][1]['value'],
                                "降雨機率":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][9]['time'][j+1]['elementValue']['value'],
                                "最大風速":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][11]['time'][j+1]['elementValue'][0]['value'],
                                "天氣現象":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][12]['time'][j+1]['elementValue'][0]['value']},ignore_index=True)
        
        df.降雨機率 = df.降雨機率.apply(lambda x : "0%" if x==None else str(x)+"%" )
        m = len(df)

        wdf = [(df.iloc[i,0],df.iloc[i,1],df.iloc[i,2],df.iloc[i,3],df.iloc[i,4],df.iloc[i,5],df.iloc[i,7],df.iloc[i,6],df.iloc[i,9],df.iloc[i,10],df.iloc[i,11],df.iloc[i,8]) for i in range(m)]

        for i in wdf:
            try:
                cursor.execute(mySql_insert_query,i)
            except:
                continue

        conn.commit()

        print("{}_data in {} insert success".format("Beach_week",arrow.now().format('YYYY/MM/DD HH:mm:ss')))

        cursor.close()

except Exception as ex:
    print(ex)




#########################################################################

# 海釣(一週日夜)

data = get_fileapi_form_cwb("F-B0053-021.json")

try:
    conn = pymysql.connect(**db_settings)

    with conn.cursor() as cursor:

        sql ="""CREATE TABLE IF NOT EXISTS Fishing_one_week (
             id int(40) NOT NULL auto_increment UNIQUE,
             Observation_site VARCHAR(20) NOT NULL, 
             Instant_time Datetime NOT NULL, 
             Lon Float(10) NOT NULL, 
             Lat Float(10) NOT NULL,
             Average_temperature VARCHAR(20) NOT NULL ,
             Average_relative_humidity VARCHAR(20) NOT NULL ,
             Minimum_temperature VARCHAR(20) NOT NULL ,
             Maximum_temperature VARCHAR(20) NOT NULL ,
             Chance_of_rain  VARCHAR(20) NOT NULL ,
             Maximum_wind_speed  VARCHAR(20) NOT NULL ,
             Weather_phenomenon VARCHAR(20) NOT NULL ,
             Maximum_comfort_index VARCHAR(20) NOT NULL,
             PRIMARY KEY (Observation_site, Instant_time))"""
        cursor.execute(sql)

        mySql_insert_query = """INSERT INTO Fishing_one_week (Observation_site,Instant_time,Lon,Lat,Average_temperature,Average_relative_humidity, Minimum_temperature,
                                                             Maximum_temperature,Chance_of_rain,Maximum_wind_speed,Weather_phenomenon,Maximum_comfort_index) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """

        df = pd.DataFrame([])

        for i in range(len(data['cwbopendata']['dataset']['locations']['location'])):
            for j in range(len(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'])-1):
                df = df.append({"地點":data['cwbopendata']['dataset']['locations']['location'][i]['locationName'],
                                "時間":arrow.get(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j+1]['startTime']).format("YYYY-MM-DD HH:mm:SS"),
                                "經度":data['cwbopendata']['dataset']['locations']['location'][i]['lon'],
                                "緯度":data['cwbopendata']['dataset']['locations']['location'][i]['lat'],
                                "平均溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j+1]['elementValue']['value']+"°C",
                                "平均相對濕度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][2]['time'][j+1]['elementValue']['value']+"%",
                                "最高溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][3]['time'][j+1]['elementValue']['value']+"°C",
                                "最低溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][4]['time'][j+1]['elementValue']['value']+"°C",
                                "最大舒適度指數":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][7]['time'][j+1]['elementValue'][1]['value'],
                                "降雨機率":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][9]['time'][j+1]['elementValue']['value'],
                                "最大風速":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][11]['time'][j+1]['elementValue'][0]['value'],
                                "天氣現象":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][12]['time'][j+1]['elementValue'][0]['value']},ignore_index=True)
        
        df.降雨機率 = df.降雨機率.apply(lambda x : "0%" if x==None else str(x)+"%" )
        m = len(df)

        wdf = [(df.iloc[i,0],df.iloc[i,1],df.iloc[i,2],df.iloc[i,3],df.iloc[i,4],df.iloc[i,5],df.iloc[i,7],df.iloc[i,6],df.iloc[i,9],df.iloc[i,10],df.iloc[i,11],df.iloc[i,8]) for i in range(m)]

        for i in wdf:
            try:
                cursor.execute(mySql_insert_query,i)
            except:
                continue

        conn.commit()

        print("{}_data in {} insert success".format("Fishing_week",arrow.now().format('YYYY/MM/DD HH:mm:ss')))

        cursor.close()

except Exception as ex:
    print(ex)




#########################################################################

# 娛樂漁業(一週日夜)

data = get_fileapi_form_cwb("F-B0053-027.json")

try:
    conn = pymysql.connect(**db_settings)

    with conn.cursor() as cursor:

        sql ="""CREATE TABLE IF NOT EXISTS Entertainment_one_week (
             id int(40) NOT NULL auto_increment UNIQUE,
             Observation_site VARCHAR(20) NOT NULL, 
             Instant_time Datetime NOT NULL, 
             Lon Float(10) NOT NULL, 
             Lat Float(10) NOT NULL,
             Average_temperature VARCHAR(20) NOT NULL ,
             Average_relative_humidity VARCHAR(20) NOT NULL ,
             Minimum_temperature VARCHAR(20) NOT NULL ,
             Maximum_temperature VARCHAR(20) NOT NULL ,
             Chance_of_rain  VARCHAR(20) NOT NULL ,
             Maximum_wind_speed  VARCHAR(20) NOT NULL ,
             Weather_phenomenon VARCHAR(20) NOT NULL ,
             Maximum_comfort_index VARCHAR(20) NOT NULL,
             PRIMARY KEY (Observation_site, Instant_time))"""
        cursor.execute(sql)

        mySql_insert_query = """INSERT INTO Entertainment_one_week (Observation_site,Instant_time,Lon,Lat,Average_temperature,Average_relative_humidity, Minimum_temperature,
                                                             Maximum_temperature,Chance_of_rain,Maximum_wind_speed,Weather_phenomenon,Maximum_comfort_index) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """

        df = pd.DataFrame([])

        for i in range(len(data['cwbopendata']['dataset']['locations']['location'])):
            for j in range(len(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'])-1):
                df = df.append({"地點":data['cwbopendata']['dataset']['locations']['location'][i]['locationName'],
                                "時間":arrow.get(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j+1]['startTime']).format("YYYY-MM-DD HH:mm:SS"),
                                "經度":data['cwbopendata']['dataset']['locations']['location'][i]['lon'],
                                "緯度":data['cwbopendata']['dataset']['locations']['location'][i]['lat'],
                                "平均溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j+1]['elementValue']['value']+"°C",
                                "平均相對濕度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][2]['time'][j+1]['elementValue']['value']+"%",
                                "最高溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][3]['time'][j+1]['elementValue']['value']+"°C",
                                "最低溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][4]['time'][j+1]['elementValue']['value']+"°C",
                                "最大舒適度指數":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][7]['time'][j+1]['elementValue'][1]['value'],
                                "降雨機率":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][9]['time'][j+1]['elementValue']['value'],
                                "最大風速":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][11]['time'][j+1]['elementValue'][0]['value'],
                                "天氣現象":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][12]['time'][j+1]['elementValue'][0]['value']},ignore_index=True)
        
        df.降雨機率 = df.降雨機率.apply(lambda x : "0%" if x==None else str(x)+"%" )
        m = len(df)

        wdf = [(df.iloc[i,0],df.iloc[i,1],df.iloc[i,2],df.iloc[i,3],df.iloc[i,4],df.iloc[i,5],df.iloc[i,7],df.iloc[i,6],df.iloc[i,9],df.iloc[i,10],df.iloc[i,11],df.iloc[i,8]) for i in range(m)]

        for i in wdf:
            try:
                cursor.execute(mySql_insert_query,i)
            except:
                continue

        conn.commit()

        print("{}_data in {} insert success".format("Entertainment_week",arrow.now().format('YYYY/MM/DD HH:mm:ss')))

        cursor.close()

except Exception as ex:
    print(ex)




#########################################################################

# 港口(一週日夜)

data = get_fileapi_form_cwb("F-B0053-051.json")

try:
    conn = pymysql.connect(**db_settings)

    with conn.cursor() as cursor:

        sql ="""CREATE TABLE IF NOT EXISTS Port_one_week (
             id int(40) NOT NULL auto_increment UNIQUE,
             Observation_site VARCHAR(20) NOT NULL, 
             Instant_time Datetime NOT NULL, 
             Lon Float(10) NOT NULL, 
             Lat Float(10) NOT NULL,
             Average_temperature VARCHAR(20) NOT NULL ,
             Average_relative_humidity VARCHAR(20) NOT NULL ,
             Minimum_temperature VARCHAR(20) NOT NULL ,
             Maximum_temperature VARCHAR(20) NOT NULL ,
             Chance_of_rain  VARCHAR(20) NOT NULL ,
             Maximum_wind_speed  VARCHAR(20) NOT NULL ,
             Weather_phenomenon VARCHAR(20) NOT NULL ,
             Maximum_comfort_index VARCHAR(20) NOT NULL,
             PRIMARY KEY (Observation_site, Instant_time))"""
        cursor.execute(sql)

        mySql_insert_query = """INSERT INTO Port_one_week (Observation_site,Instant_time,Lon,Lat,Average_temperature,Average_relative_humidity, Minimum_temperature,
                                                             Maximum_temperature,Chance_of_rain,Maximum_wind_speed,Weather_phenomenon,Maximum_comfort_index) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """

        df = pd.DataFrame([])

        for i in range(len(data['cwbopendata']['dataset']['locations']['location'])):
            for j in range(len(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'])-1):
                df = df.append({"地點":data['cwbopendata']['dataset']['locations']['location'][i]['locationName'],
                                "時間":arrow.get(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j+1]['startTime']).format("YYYY-MM-DD HH:mm:SS"),
                                "經度":data['cwbopendata']['dataset']['locations']['location'][i]['lon'],
                                "緯度":data['cwbopendata']['dataset']['locations']['location'][i]['lat'],
                                "平均溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j+1]['elementValue']['value']+"°C",
                                "平均相對濕度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][2]['time'][j+1]['elementValue']['value']+"%",
                                "最高溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][3]['time'][j+1]['elementValue']['value']+"°C",
                                "最低溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][4]['time'][j+1]['elementValue']['value']+"°C",
                                "最大舒適度指數":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][7]['time'][j+1]['elementValue'][1]['value'],
                                "降雨機率":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][9]['time'][j+1]['elementValue']['value'],
                                "最大風速":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][11]['time'][j+1]['elementValue'][0]['value'],
                                "天氣現象":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][12]['time'][j+1]['elementValue'][0]['value']},ignore_index=True)
        
        df.降雨機率 = df.降雨機率.apply(lambda x : "0%" if x==None else str(x)+"%" )
        m = len(df)

        wdf = [(df.iloc[i,0],df.iloc[i,1],df.iloc[i,2],df.iloc[i,3],df.iloc[i,4],df.iloc[i,5],df.iloc[i,7],df.iloc[i,6],df.iloc[i,9],df.iloc[i,10],df.iloc[i,11],df.iloc[i,8]) for i in range(m)]

        for i in wdf:
            try:
                cursor.execute(mySql_insert_query,i)
            except:
                continue

        conn.commit()

        print("{}_data in {} insert success".format("Port_week",arrow.now().format('YYYY/MM/DD HH:mm:ss')))

        cursor.close()

except Exception as ex:
    print(ex)


#########################################################################

# 浮潛(一週日夜)

data = get_fileapi_form_cwb("F-B0053-076.json")

try:
    conn = pymysql.connect(**db_settings)

    with conn.cursor() as cursor:

        sql ="""CREATE TABLE IF NOT EXISTS Snorkeling_one_week (
             id int(40) NOT NULL auto_increment UNIQUE,
             Observation_site VARCHAR(20) NOT NULL, 
             Instant_time Datetime NOT NULL, 
             Lon Float(10) NOT NULL, 
             Lat Float(10) NOT NULL,
             Average_temperature VARCHAR(20) NOT NULL ,
             Average_relative_humidity VARCHAR(20) NOT NULL ,
             Minimum_temperature VARCHAR(20) NOT NULL ,
             Maximum_temperature VARCHAR(20) NOT NULL ,
             Chance_of_rain  VARCHAR(20) NOT NULL ,
             Maximum_wind_speed  VARCHAR(20) NOT NULL ,
             Weather_phenomenon VARCHAR(20) NOT NULL ,
             Maximum_comfort_index VARCHAR(20) NOT NULL,
             PRIMARY KEY (Observation_site, Instant_time))"""
        cursor.execute(sql)

        mySql_insert_query = """INSERT INTO Snorkeling_one_week (Observation_site,Instant_time,Lon,Lat,Average_temperature,Average_relative_humidity, Minimum_temperature,
                                                             Maximum_temperature,Chance_of_rain,Maximum_wind_speed,Weather_phenomenon,Maximum_comfort_index) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """

        df = pd.DataFrame([])

        for i in range(len(data['cwbopendata']['dataset']['locations']['location'])):
            for j in range(len(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'])-1):
                df = df.append({"地點":data['cwbopendata']['dataset']['locations']['location'][i]['locationName'],
                                "時間":arrow.get(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j+1]['startTime']).format("YYYY-MM-DD HH:mm:SS"),
                                "經度":data['cwbopendata']['dataset']['locations']['location'][i]['lon'],
                                "緯度":data['cwbopendata']['dataset']['locations']['location'][i]['lat'],
                                "平均溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j+1]['elementValue']['value']+"°C",
                                "平均相對濕度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][2]['time'][j+1]['elementValue']['value']+"%",
                                "最高溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][3]['time'][j+1]['elementValue']['value']+"°C",
                                "最低溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][4]['time'][j+1]['elementValue']['value']+"°C",
                                "最大舒適度指數":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][7]['time'][j+1]['elementValue'][1]['value'],
                                "降雨機率":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][9]['time'][j+1]['elementValue']['value'],
                                "最大風速":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][11]['time'][j+1]['elementValue'][0]['value'],
                                "天氣現象":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][12]['time'][j+1]['elementValue'][0]['value']},ignore_index=True)
        
        df.降雨機率 = df.降雨機率.apply(lambda x : "0%" if x==None else str(x)+"%" )
        m = len(df)

        wdf = [(df.iloc[i,0],df.iloc[i,1],df.iloc[i,2],df.iloc[i,3],df.iloc[i,4],df.iloc[i,5],df.iloc[i,7],df.iloc[i,6],df.iloc[i,9],df.iloc[i,10],df.iloc[i,11],df.iloc[i,8]) for i in range(m)]

        for i in wdf:
            try:
                cursor.execute(mySql_insert_query,i)
            except:
                continue

        conn.commit()

        print("{}_data in {} insert success".format("Snorkeling_week",arrow.now().format('YYYY/MM/DD HH:mm:ss')))

        cursor.close()

except Exception as ex:
    print(ex)




#########################################################################

# 衝浪(一週日夜)

data = get_fileapi_form_cwb("F-B0053-082.json")

try:
    conn = pymysql.connect(**db_settings)

    with conn.cursor() as cursor:

        sql ="""CREATE TABLE IF NOT EXISTS Surfing_one_week (
             id int(40) NOT NULL auto_increment UNIQUE,
             Observation_site VARCHAR(20) NOT NULL, 
             Instant_time Datetime NOT NULL, 
             Lon Float(10) NOT NULL, 
             Lat Float(10) NOT NULL,
             Average_temperature VARCHAR(20) NOT NULL ,
             Average_relative_humidity VARCHAR(20) NOT NULL ,
             Minimum_temperature VARCHAR(20) NOT NULL ,
             Maximum_temperature VARCHAR(20) NOT NULL ,
             Chance_of_rain  VARCHAR(20) NOT NULL ,
             Maximum_wind_speed  VARCHAR(20) NOT NULL ,
             Weather_phenomenon VARCHAR(20) NOT NULL ,
             Maximum_comfort_index VARCHAR(20) NOT NULL,
             PRIMARY KEY (Observation_site, Instant_time))"""
        cursor.execute(sql)

        mySql_insert_query = """INSERT INTO Surfing_one_week (Observation_site,Instant_time,Lon,Lat,Average_temperature,Average_relative_humidity, Minimum_temperature,
                                                             Maximum_temperature,Chance_of_rain,Maximum_wind_speed,Weather_phenomenon,Maximum_comfort_index) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """

        df = pd.DataFrame([])

        for i in range(len(data['cwbopendata']['dataset']['locations']['location'])):
            for j in range(len(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'])-1):
                df = df.append({"地點":data['cwbopendata']['dataset']['locations']['location'][i]['locationName'],
                                "時間":arrow.get(data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j+1]['startTime']).format("YYYY-MM-DD HH:mm:SS"),
                                "經度":data['cwbopendata']['dataset']['locations']['location'][i]['lon'],
                                "緯度":data['cwbopendata']['dataset']['locations']['location'][i]['lat'],
                                "平均溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][0]['time'][j+1]['elementValue']['value']+"°C",
                                "平均相對濕度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][2]['time'][j+1]['elementValue']['value']+"%",
                                "最高溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][3]['time'][j+1]['elementValue']['value']+"°C",
                                "最低溫度":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][4]['time'][j+1]['elementValue']['value']+"°C",
                                "最大舒適度指數":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][7]['time'][j+1]['elementValue'][1]['value'],
                                "降雨機率":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][9]['time'][j+1]['elementValue']['value'],
                                "最大風速":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][11]['time'][j+1]['elementValue'][0]['value'],
                                "天氣現象":data['cwbopendata']['dataset']['locations']['location'][i]['weatherElement'][12]['time'][j+1]['elementValue'][0]['value']},ignore_index=True)
        
        df.降雨機率 = df.降雨機率.apply(lambda x : "0%" if x==None else str(x)+"%" )
        m = len(df)

        wdf = [(df.iloc[i,0],df.iloc[i,1],df.iloc[i,2],df.iloc[i,3],df.iloc[i,4],df.iloc[i,5],df.iloc[i,7],df.iloc[i,6],df.iloc[i,9],df.iloc[i,10],df.iloc[i,11],df.iloc[i,8]) for i in range(m)]

        for i in wdf:
            try:
                cursor.execute(mySql_insert_query,i)
            except:
                continue

        conn.commit()

        print("{}_data in {} insert success".format("Surfing_weeking",arrow.now().format('YYYY/MM/DD HH:mm:ss')))

        cursor.close()

except Exception as ex:
    print(ex)