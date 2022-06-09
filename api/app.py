# Basic
import arrow
import pandas as pd
import numpy as np
import re

# Flask
from database import Database

from flask import Flask 
from flask import render_template
from flask import jsonify
from flask import request
from flask_cors import CORS # CORS(Cross-Origin Resource Sharing 跨來源資源共享，讓網站可以去存取不同跨網域伺服器的資源
from flasgger import Swagger

app = Flask(__name__)
app.config['SWAGGER'] = {
    "title": "海是會害怕API",
    "description": "海是會害怕API測試",
    "version": "1.0",
    "termsOfService": "",
    "hide_top_bar": True
}
CORS(app)
Swagger(app)

@app.route("/")
def weather():
    return render_template("weather.html")

### 氣象及海象查詢 ################################### 育樂天氣預報資料(當天) ######################################

@app.route("/search_location/today",methods=["GET"])
def show_today():
    """
    育樂天氣預報資料(當天)
    ---
    tags:
        - search_location
    parameters:
      - in: query
        default: 小灣
        name: location
        type: string
        required: true
        description: 水域地點
      - in: query
        name: type
        enum: ['海水浴場','娛樂漁業','海釣','港口','浮潛','衝浪']
        type: string
        required: true
        description: 水域種類
    responses:
      500:
        description: Fail
      200:
        description: Success
    """
    #首先獲取前端傳入的name資料
  
    location = request.args.get("location")
    type = request.args.get("type")

    sql = Database("data_contest")
    datetime = arrow.now().shift(hours=+8).format("YYYY-MM-DD")

    if type == "海水浴場":

        try:
            #執行sql陳述句 多說一句，f+字串的形式，可以在字串里面以{}的形式加入變數名 結果保存在result陣列中
            search_result = sql.execute(f"SELECT id , Instant_time , temperature , body_temperature , relative_humidity , Chance_of_rain , Maximum_wind_speed , Weather_phenomenon , Maximum_comfort_index  FROM Beach_three_day WHERE Observation_site='{location}' AND Instant_time >='{datetime}' ORDER BY `id` ASC LIMIT 8")
            #print(pd.DataFrame(search_result))
        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}

        else:
            if not len(search_result) == 0:
                keys = ['id', 'Instant_time', 'temperature', 'body_temperature', 'relative_humidity', 'Chance_of_rain', 'Maximum_wind_speed', 'Weather_phenomenon', 'Maximum_comfort_index']
                results = [dict(zip(keys, values)) for values in search_result]
                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"

    elif type == "娛樂漁業":

        try:
            search_result = sql.execute(f"SELECT id , Instant_time , temperature , body_temperature , relative_humidity , Chance_of_rain , Maximum_wind_speed , Weather_phenomenon , Maximum_comfort_index  FROM Entertainment_three_day WHERE Observation_site='{location}' AND Instant_time>='{datetime}' ORDER BY `id` ASC LIMIT 8")
        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}
        else:
            if not len(search_result) == 0:
                keys = ['id', 'Instant_time', 'temperature', 'body_temperature', 'relative_humidity', 'Chance_of_rain', 'Maximum_wind_speed', 'Weather_phenomenon', 'Maximum_comfort_index']
                results = [dict(zip(keys, values)) for values in search_result]
                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"

    elif type == "海釣":
    
        try:
            search_result = sql.execute(f"SELECT id , Instant_time , temperature , body_temperature , relative_humidity , Chance_of_rain , Maximum_wind_speed , Weather_phenomenon , Maximum_comfort_index  FROM Fishing_three_day WHERE Observation_site='{location}' AND Instant_time>='{datetime}' ORDER BY `id` ASC LIMIT 8")
        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}
        else:
            if not len(search_result) == 0:
                keys = ['id', 'Instant_time', 'temperature', 'body_temperature', 'relative_humidity', 'Chance_of_rain', 'Maximum_wind_speed', 'Weather_phenomenon', 'Maximum_comfort_index']
                results = [dict(zip(keys, values)) for values in search_result]
                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"

    elif type == "港口":
    
        try:
            search_result = sql.execute(f"SELECT id , Instant_time , temperature , body_temperature , relative_humidity , Chance_of_rain , Maximum_wind_speed , Weather_phenomenon , Maximum_comfort_index  FROM Port_three_day  WHERE Observation_site='{location}' AND Instant_time>='{datetime}' ORDER BY `id` ASC LIMIT 8")
        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}
        else:
            if not len(search_result) == 0:
                keys = ['id', 'Instant_time', 'temperature', 'body_temperature', 'relative_humidity', 'Chance_of_rain', 'Maximum_wind_speed', 'Weather_phenomenon', 'Maximum_comfort_index']
                results = [dict(zip(keys, values)) for values in search_result]
                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"

    elif type == "浮潛":
    
        try:
            search_result = sql.execute(f"SELECT id , Instant_time , temperature , body_temperature , relative_humidity , Chance_of_rain , Maximum_wind_speed , Weather_phenomenon , Maximum_comfort_index  FROM Snorkeling_three_day WHERE Observation_site='{location}' AND Instant_time>='{datetime}' ORDER BY `id` ASC LIMIT 8")
        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}
        else:
            if not len(search_result) == 0:
                keys = ['id', 'Instant_time', 'temperature', 'body_temperature', 'relative_humidity', 'Chance_of_rain', 'Maximum_wind_speed', 'Weather_phenomenon', 'Maximum_comfort_index']
                results = [dict(zip(keys, values)) for values in search_result]
                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"

    else :
    
        try:
            search_result = sql.execute(f"SELECT id , Instant_time , temperature , body_temperature , relative_humidity , Chance_of_rain , Maximum_wind_speed , Weather_phenomenon , Maximum_comfort_index  FROM Surfing_three_day WHERE Observation_site='{location}' AND Instant_time>='{datetime}' ORDER BY `id` ASC LIMIT 8")
        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}
        else:
            if not len(search_result) == 0:
                keys = ['id', 'Instant_time', 'temperature', 'body_temperature', 'relative_humidity', 'Chance_of_rain', 'Maximum_wind_speed', 'Weather_phenomenon', 'Maximum_comfort_index']
                results = [dict(zip(keys, values)) for values in search_result]
                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"


### 氣象及海象查詢 ################################### 育樂天氣預報資料(三天3小時) ######################################

@app.route("/search_location/threeday",methods=["GET"])
def show_threeday():
    """
    育樂天氣預報資料(三天3小時)
    ---
    tags:
        - search_location
    parameters:
      - in: query
        default: 小灣
        name: location
        type: string
        required: true
        description: 水域地點
      - in: query
        name: type
        enum: ['海水浴場','娛樂漁業','海釣','港口','浮潛','衝浪']
        type: string
        required: true
        description: 水域種類
    responses:
      500:
        description: Fail
      200:
        description: Success
    """
    #首先獲取前端傳入的name資料

    location = request.args.get("location")
    type = request.args.get("type")

    sql = Database("data_contest")
    datetime = arrow.now().shift(hours=+8).format("YYYY-MM-DD HH:mm:ss")

    if type == "海水浴場":

        try:
            #執行sql陳述句 多說一句，f+字串的形式，可以在字串里面以{}的形式加入變數名 結果保存在result陣列中
            search_result = sql.execute(f"SELECT id , Instant_time , temperature , body_temperature , relative_humidity , Chance_of_rain , Maximum_wind_speed , Weather_phenomenon , Maximum_comfort_index FROM Beach_three_day WHERE Observation_site='{location}' AND Instant_time>='{datetime}' ORDER BY `id` ASC LIMIT 24")
            #print(pd.DataFrame(search_result))
        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}

        else:
            if not len(search_result) == 0:
                keys = ['id', 'Instant_time', 'temperature', 'body_temperature', 'relative_humidity', 'Chance_of_rain', 'Maximum_wind_speed', 'Weather_phenomenon', 'Maximum_comfort_index']
                results = [dict(zip(keys, values)) for values in search_result]
                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"

    elif type == "娛樂漁業":

        try:
            search_result = sql.execute(f"SELECT id , Instant_time , temperature , body_temperature , relative_humidity , Chance_of_rain , Maximum_wind_speed , Weather_phenomenon , Maximum_comfort_index FROM Entertainment_three_day WHERE Observation_site='{location}' AND Instant_time>='{datetime}' ORDER BY `id` ASC LIMIT 24")
        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}
        else:
            if not len(search_result) == 0:
                keys = ['id', 'Instant_time', 'temperature', 'body_temperature', 'relative_humidity', 'Chance_of_rain', 'Maximum_wind_speed', 'Weather_phenomenon', 'Maximum_comfort_index']
                results = [dict(zip(keys, values)) for values in search_result]
                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"

    elif type == "海釣":
    
        try:
            search_result = sql.execute(f"SELECT id , Instant_time , temperature , body_temperature , relative_humidity , Chance_of_rain , Maximum_wind_speed , Weather_phenomenon , Maximum_comfort_index FROM Fishing_three_day WHERE Observation_site='{location}' AND Instant_time>='{datetime}' ORDER BY `id` ASC LIMIT 24")
        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}
        else:
            if not len(search_result) == 0:
                keys = ['id', 'Instant_time', 'temperature', 'body_temperature', 'relative_humidity', 'Chance_of_rain', 'Maximum_wind_speed', 'Weather_phenomenon', 'Maximum_comfort_index']
                results = [dict(zip(keys, values)) for values in search_result]
                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"

    elif type == "港口":
    
        try:
            search_result = sql.execute(f"SELECT id , Instant_time , temperature , body_temperature , relative_humidity , Chance_of_rain , Maximum_wind_speed , Weather_phenomenon , Maximum_comfort_index FROM Port_three_day  WHERE Observation_site='{location}' AND Instant_time>='{datetime}' ORDER BY `id` ASC LIMIT 24")
        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}
        else:
            if not len(search_result) == 0:
                keys = ['id', 'Instant_time', 'temperature', 'body_temperature', 'relative_humidity', 'Chance_of_rain', 'Maximum_wind_speed', 'Weather_phenomenon', 'Maximum_comfort_index']
                results = [dict(zip(keys, values)) for values in search_result]
                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"

    elif type == "浮潛":
    
        try:
            search_result = sql.execute(f"SELECT id , Instant_time , temperature , body_temperature , relative_humidity , Chance_of_rain , Maximum_wind_speed , Weather_phenomenon , Maximum_comfort_index FROM Snorkeling_three_day WHERE Observation_site='{location}' AND Instant_time>='{datetime}' ORDER BY `id` ASC LIMIT 24")
        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}
        else:
            if not len(search_result) == 0:
                keys = ['id', 'Instant_time', 'temperature', 'body_temperature', 'relative_humidity', 'Chance_of_rain', 'Maximum_wind_speed', 'Weather_phenomenon', 'Maximum_comfort_index']
                results = [dict(zip(keys, values)) for values in search_result]
                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"

    else :
    
        try:
            search_result = sql.execute(f"SELECT id , Instant_time , temperature , body_temperature , relative_humidity , Chance_of_rain , Maximum_wind_speed , Weather_phenomenon , Maximum_comfort_index FROM Surfing_three_day WHERE Observation_site='{location}' AND Instant_time>='{datetime}' ORDER BY `id` ASC LIMIT 24")
        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}
        else:
            if not len(search_result) == 0:
                keys = ['id', 'Instant_time', 'temperature', 'body_temperature', 'relative_humidity', 'Chance_of_rain', 'Maximum_wind_speed', 'Weather_phenomenon', 'Maximum_comfort_index']
                results = [dict(zip(keys, values)) for values in search_result]
                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"



### 氣象及海象查詢 ################################### 育樂天氣預報資料(一週日夜) ######################################


@app.route("/search_location/oneweek",methods=["GET"])
def show_oneweek():
    """
    育樂天氣預報資料(一週日夜)
    ---
    tags:
        - search_location
    parameters:
      - in: query
        default: 小灣
        name: location
        type: string
        required: true
        description: 水域地點
      - in: query
        name: type
        enum: ['海水浴場','娛樂漁業','海釣','港口','浮潛','衝浪']
        type: string
        required: true
        description: 水域種類
    responses:
      500:
        description: Fail
      200:
        description: Success
    """
    #首先獲取前端傳入的name資料

    location = request.args.get("location")
    type = request.args.get("type")

    sql = Database("data_contest")
    datetime = arrow.now().shift(hours=+8).format("YYYY-MM-DD HH:mm:ss")

    if type == "海水浴場":

        try:
            #執行sql陳述句 多說一句，f+字串的形式，可以在字串里面以{}的形式加入變數名 結果保存在result陣列中
            search_result = sql.execute(f"SELECT id , Instant_time , Average_temperature , 	Average_relative_humidity , Minimum_temperature , Maximum_temperature , Chance_of_rain , Maximum_wind_speed , Weather_phenomenon , Maximum_comfort_index FROM Beach_one_week WHERE Observation_site='{location}' AND Instant_time>='{datetime}' ORDER BY `id` ASC LIMIT 14")
            #print(pd.DataFrame(search_result))
        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}

        else:
            if not len(search_result) == 0:
                print(search_result)
                keys = ['id', 'Instant_time', 'Average_temperature', 'Average_relative_humidity', 'Minimum_temperature', 'Maximum_temperature', 'Chance_of_rain', 'Maximum_wind_speed', 'Weather_phenomenon', 'Maximum_comfort_index']
                results = [dict(zip(keys, values)) for values in search_result]
                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"

    elif type == "娛樂漁業":

        try:
            search_result = sql.execute(f"SELECT id , Instant_time , Average_temperature , 	Average_relative_humidity , Minimum_temperature , Maximum_temperature , Chance_of_rain , Maximum_wind_speed , Weather_phenomenon , Maximum_comfort_index FROM Entertainment_one_week WHERE Observation_site='{location}' AND Instant_time>='{datetime}' ORDER BY `id` ASC LIMIT 14")
        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}
        else:
            if not len(search_result) == 0:
                keys = ['id', 'Instant_time', 'Average_temperature', 'Average_relative_humidity', 'Minimum_temperature', 'Maximum_temperature', 'Chance_of_rain', 'Maximum_wind_speed', 'Weather_phenomenon', 'Maximum_comfort_index']
                results = [dict(zip(keys, values)) for values in search_result]
                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"

    elif type == "海釣":
    
        try:
            search_result = sql.execute(f"SELECT id , Instant_time , Average_temperature , 	Average_relative_humidity , Minimum_temperature , Maximum_temperature , Chance_of_rain , Maximum_wind_speed , Weather_phenomenon , Maximum_comfort_index FROM Fishing_one_week WHERE Observation_site='{location}' AND Instant_time>='{datetime}' ORDER BY `id` ASC LIMIT 14")
        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}
        else:
            if not len(search_result) == 0:
                keys = ['id', 'Instant_time', 'Average_temperature', 'Average_relative_humidity', 'Minimum_temperature', 'Maximum_temperature', 'Chance_of_rain', 'Maximum_wind_speed', 'Weather_phenomenon', 'Maximum_comfort_index']
                results = [dict(zip(keys, values)) for values in search_result]
                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"

    elif type == "港口":
    
        try:
            search_result = sql.execute(f"SELECT id , Instant_time , Average_temperature , 	Average_relative_humidity , Minimum_temperature , Maximum_temperature , Chance_of_rain , Maximum_wind_speed , Weather_phenomenon , Maximum_comfort_index FROM Port_one_week  WHERE Observation_site='{location}' AND Instant_time>='{datetime}' ORDER BY `id` ASC LIMIT 14")
        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}
        else:
            if not len(search_result) == 0:
                keys = ['id', 'Instant_time', 'Average_temperature', 'Average_relative_humidity', 'Minimum_temperature', 'Maximum_temperature', 'Chance_of_rain', 'Maximum_wind_speed', 'Weather_phenomenon', 'Maximum_comfort_index']
                results = [dict(zip(keys, values)) for values in search_result]
                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"

    elif type == "浮潛":
    
        try:
            search_result = sql.execute(f"SELECT id , Instant_time , Average_temperature , 	Average_relative_humidity , Minimum_temperature , Maximum_temperature , Chance_of_rain , Maximum_wind_speed , Weather_phenomenon , Maximum_comfort_index FROM Snorkeling_one_week WHERE Observation_site='{location}' AND Instant_time>='{datetime}' ORDER BY `id` ASC LIMIT 14")
        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}
        else:
            if not len(search_result) == 0:
                keys = ['id', 'Instant_time', 'Average_temperature', 'Average_relative_humidity', 'Minimum_temperature', 'Maximum_temperature', 'Chance_of_rain', 'Maximum_wind_speed', 'Weather_phenomenon', 'Maximum_comfort_index']
                results = [dict(zip(keys, values)) for values in search_result]
                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"

    else :
    
        try:
            search_result = sql.execute(f"SELECT id , Instant_time , Average_temperature , 	Average_relative_humidity , Minimum_temperature , Maximum_temperature , Chance_of_rain , Maximum_wind_speed , Weather_phenomenon , Maximum_comfort_index FROM Surfing_one_week WHERE Observation_site='{location}' AND Instant_time>='{datetime}' ORDER BY `id` ASC LIMIT 14")
        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}
        else:
            if not len(search_result) == 0:
                keys = ['id', 'Instant_time', 'Average_temperature', 'Average_relative_humidity', 'Minimum_temperature', 'Maximum_temperature', 'Chance_of_rain', 'Maximum_wind_speed', 'Weather_phenomenon', 'Maximum_comfort_index']
                results = [dict(zip(keys, values)) for values in search_result]
                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"

### 適合度推薦 ################################### 育樂天氣預報資料(一週三天) ######################################

def hint_weather(temp,rain):
    
    if temp <= 14 and rain >= 50 :
        return "天氣寒冷且高機率會下雨，是個極度濕冷的天氣，麻煩多加幾件外套和攜帶雨具，請隨時注意自身身體狀況以及注意保暖"

    elif temp >= 28 :
        return "天氣炎熱，做好防曬對抗紫外線，請隨時注意自身身體狀況以及補充水分"

    elif temp <= 14 :
        return "天氣寒冷，麻煩多加幾件外套，請隨時注意自身身體狀況以及注意保暖"

    elif rain >= 70 and temp <= 24 and temp >= 18 :
        return "溫度適中，但降雨機率還是偏高，請記得攜帶雨具"
    
    elif rain >= 70 and temp > 24  :
        return "溫度偏高，且降雨機率也偏高，會是個濕熱的天氣，請記得適時補充水分和攜帶雨具。"

    elif rain >= 70 and temp < 18  :
        return "溫度偏低，而降雨機率偏高，會是個濕冷的天氣，請記得帶件薄外套和雨具。"

    elif rain >= 40 and temp <= 24 and temp >= 18 :
        return "溫度適中，注意局部地方降雨，請記得攜帶雨具"

    elif rain >= 40 and temp > 24  :
        return "溫度偏高，注意局部地方降雨，請記得適時補充水分和攜帶雨具。"

    elif rain >= 40 and temp < 18  :
        return "溫度偏低，注意局部地方降雨，請記得帶件薄外套和雨具。"
    
    elif rain >= 10 and temp <= 24 and temp >= 18 :
        return "溫度適中，但可能會有短暫陣雨，是個很適合出門活動的時間"

    elif rain >= 10 and temp > 24  :
        return "溫度偏高，可能會有短暫陣雨，請記得適時補充水分，是個適合出門活動的時間"

    elif rain >= 10 and temp < 18  :
        return "溫度偏低，可能會有短暫陣雨，請記得帶件薄外套，是個適合出門活動的時間"

    elif rain < 10 and temp <= 24 and temp >= 18 :
        return "溫度適中，原則上不會下雨，是個非常適合出門活動的時間"

    elif rain < 10 and temp > 24  :
        return "溫度偏高，但原則上不會下雨，會是個乾熱的天氣，請記得適時補充水分，是個很適合出門活動的時間"

    elif rain < 10 and temp < 18  :
        return "溫度偏低，但原則上不會下雨，會是個乾冷的天氣，請記得帶件薄外套，是個很適合出門活動的時間"
    else :
        return None

@app.route("/recommend",methods=["GET"])
def show_recommend():
    """
    育樂天氣預報資料(一週三天)
    ---
    tags:
        - recommend
    parameters:
      - in: query
        default: 小灣
        name: location
        type: string
        required: true
        description: 水域地點
      - in: query
        name: type
        enum: ['海水浴場','娛樂漁業','海釣','港口','浮潛','衝浪']
        type: string
        required: true
        description: 水域種類
    responses:
      500:
        description: Fail
      200:
        description: Success
    """
    #首先獲取前端傳入的name資料

    location = request.args.get("location")
    type = request.args.get("type")
 
    #datetime = arrow.now().format("YYYY-MM-DD HH:mm:ss")
    datetime = arrow.now().shift(hours=+8).format("YYYY-MM-DD HH:mm:ss")
    datetime1 = arrow.now().shift(hours=+80).format("YYYY-MM-DD HH:mm:ss")

    sql = Database("data_contest")

    if type == "海水浴場":

        try:
            search_result = sql.execute(f"SELECT * FROM Beach_three_day WHERE Observation_site='{location}' AND Instant_time>='{datetime}' AND Instant_time<'{datetime1}' ORDER BY `id` ASC")

            search_result = pd.DataFrame(search_result)
            search_result.reset_index(drop=True,inplace=True)

            remove_index=[]
            for i,tm in enumerate(search_result.iloc[:,2].apply(str)):
                if ("21:00:00" in tm) or ("00:00:00" in tm) or ("03:00:00" in tm):
                    remove_index.append(i)

            search_result.drop(index=remove_index,inplace=True)
            search_result.reset_index(drop=True,inplace=True)
                
            df = search_result.iloc[:,[2,5,8]]
            df.columns=["instant_time","temperature","Chance_of_rain"]
        

            df["temperature"] = df["temperature"].apply(lambda x : int(re.sub("°C","",x)))
            df["Chance_of_rain"] = df["Chance_of_rain"].apply(lambda x : int(re.sub("%","",x)))

            if np.mean(df["Chance_of_rain"]) > 90 :
                return {'status':'Not recommended','search_result':"High chance of rain is not suitable for travel"}

    
            rain_index = sorted(range(len(df["Chance_of_rain"])), key=lambda k: df["Chance_of_rain"][k])[:7]
            # Comfortable temperature 19~23
            if np.median(df["temperature"]) > 23:
                comfort_index = sorted(rain_index, key=lambda k: df["temperature"][rain_index][k])[:3]

            elif np.median(df["temperature"]) < 19:
                comfort_index = sorted(rain_index, key=lambda k: df["temperature"][rain_index][k],reverse=True)[:3]

            else :
                comfort_index = sorted(rain_index, key=lambda k: df["temperature"][rain_index][k])[(round(len(rain_index)/2)-1):(round(len(rain_index)/2)+2)]

            search_result = [(df.iloc[index,0],hint_weather(df.iloc[index,1],df.iloc[index,2]))  for index in comfort_index]

            
        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}

        else:
            if not len(search_result) == 0:
    
                keys = ['Recommended_time','Recommended_hint']
                Recommended_results =  [dict(zip(keys, values)) for values in search_result]
                sort_keys = ['First_recommendation','Second recommendation','Third_recommendation']
                results =  [dict(zip(sort_keys,Recommended_results))]

                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"

    if type == "娛樂漁業":

        try:
            search_result = sql.execute(f"SELECT * FROM Entertainment_three_day WHERE Observation_site='{location}' AND Instant_time>='{datetime}' AND Instant_time<'{datetime1}' ORDER BY `id` ASC")

            search_result = pd.DataFrame(search_result)
            search_result.reset_index(drop=True,inplace=True)

            remove_index=[]
            for i,tm in enumerate(search_result.iloc[:,2].apply(str)):
                if ("21:00:00" in tm) or ("00:00:00" in tm) or ("03:00:00" in tm):
                    remove_index.append(i)

            search_result.drop(index=remove_index,inplace=True)
            search_result.reset_index(drop=True,inplace=True)

            df = search_result.iloc[:,[2,5,8]]
            df.columns=["instant_time","temperature","Chance_of_rain"]


            df["temperature"] = df["temperature"].apply(lambda x : int(re.sub("°C","",x)))
            df["Chance_of_rain"] = df["Chance_of_rain"].apply(lambda x : int(re.sub("%","",x)))

            if np.mean(df["Chance_of_rain"]) > 90 :
                return {'status':'Not recommended','search_result':"High chance of rain is not suitable for travel"}

            rain_index = sorted(range(len(df["Chance_of_rain"])), key=lambda k: df["Chance_of_rain"][k])[:7]

            # Comfortable temperature 19~23
            if np.median(df["temperature"]) > 23:
                comfort_index = sorted(rain_index, key=lambda k: df["temperature"][rain_index][k])[:3]

            elif np.median(df["temperature"]) < 19:
                comfort_index = sorted(rain_index, key=lambda k: df["temperature"][rain_index][k],reverse=True)[:3]

            else :
                comfort_index = sorted(rain_index, key=lambda k: df["temperature"][rain_index][k])[(round(len(rain_index)/2)-1):(round(len(rain_index)/2)+2)]
            
            search_result = [(df.iloc[index,0],hint_weather(df.iloc[index,1],df.iloc[index,2]))  for index in comfort_index]

        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}

        else:
            if not len(search_result) == 0:
                keys = ['Recommended_time','Recommended_hint']
                Recommended_results =  [dict(zip(keys, values)) for values in search_result]
                sort_keys = ['First_recommendation','Second recommendation','Third_recommendation']
                results =  [dict(zip(sort_keys,Recommended_results))]

                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"

    
    if type == "海釣":
    
        try:
            search_result = sql.execute(f"SELECT * FROM Fishing_three_day WHERE Observation_site='{location}' AND Instant_time>='{datetime}' AND Instant_time<'{datetime1}' ORDER BY `id` ASC")

            search_result = pd.DataFrame(search_result)
            search_result.reset_index(drop=True,inplace=True)

            remove_index=[]
            for i,tm in enumerate(search_result.iloc[:,2].apply(str)):
                if ("21:00:00" in tm) or ("00:00:00" in tm) or ("03:00:00" in tm):
                    remove_index.append(i)

            search_result.drop(index=remove_index,inplace=True)
            search_result.reset_index(drop=True,inplace=True)

            df = search_result.iloc[:,[2,5,8]]
            df.columns=["instant_time","temperature","Chance_of_rain"]
        

            df["temperature"] = df["temperature"].apply(lambda x : int(re.sub("°C","",x)))
            df["Chance_of_rain"] = df["Chance_of_rain"].apply(lambda x : int(re.sub("%","",x)))

            if np.mean(df["Chance_of_rain"]) > 90 :
                return {'status':'Not recommended','search_result':"High chance of rain is not suitable for travel"}

            rain_index = sorted(range(len(df["Chance_of_rain"])), key=lambda k: df["Chance_of_rain"][k])[:7]
            # Comfortable temperature 19~23
            if np.median(df["temperature"]) > 23:
                comfort_index = sorted(rain_index, key=lambda k: df["temperature"][rain_index][k])[:3]

            elif np.median(df["temperature"]) < 19:
                comfort_index = sorted(rain_index, key=lambda k: df["temperature"][rain_index][k],reverse=True)[:3]

            else :
                comfort_index = sorted(rain_index, key=lambda k: df["temperature"][rain_index][k])[(round(len(rain_index)/2)-1):(round(len(rain_index)/2)+2)]
            
            search_result = [(df.iloc[index,0],hint_weather(df.iloc[index,1],df.iloc[index,2]))  for index in comfort_index]

        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}

        else:
            if not len(search_result) == 0:
                keys = ['Recommended_time','Recommended_hint']
                Recommended_results =  [dict(zip(keys, values)) for values in search_result]
                sort_keys = ['First_recommendation','Second recommendation','Third_recommendation']
                results =  [dict(zip(sort_keys,Recommended_results))]

                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"


    if type == "港口":
        
        try:
            search_result = sql.execute(f"SELECT * FROM Port_three_day WHERE Observation_site='{location}' AND Instant_time>='{datetime}' AND Instant_time<'{datetime1}' ORDER BY `id` ASC")

            search_result = pd.DataFrame(search_result)
            search_result.reset_index(drop=True,inplace=True)

            remove_index=[]
            for i,tm in enumerate(search_result.iloc[:,2].apply(str)):
                if ("21:00:00" in tm) or ("00:00:00" in tm) or ("03:00:00" in tm):
                    remove_index.append(i)

            search_result.drop(index=remove_index,inplace=True)
            search_result.reset_index(drop=True,inplace=True)

            df = search_result.iloc[:,[2,5,8]]
            df.columns=["instant_time","temperature","Chance_of_rain"]
        

            df["temperature"] = df["temperature"].apply(lambda x : int(re.sub("°C","",x)))
            df["Chance_of_rain"] = df["Chance_of_rain"].apply(lambda x : int(re.sub("%","",x)))

            if np.mean(df["Chance_of_rain"]) > 90 :
                return {'status':'Not recommended','search_result':"High chance of rain is not suitable for travel"}

            rain_index = sorted(range(len(df["Chance_of_rain"])), key=lambda k: df["Chance_of_rain"][k])[:7]

            # Comfortable temperature 19~23
            if np.median(df["temperature"]) > 23:
                comfort_index = sorted(rain_index, key=lambda k: df["temperature"][rain_index][k])[:3]

            elif np.median(df["temperature"]) < 19:
                comfort_index = sorted(rain_index, key=lambda k: df["temperature"][rain_index][k],reverse=True)[:3]

            else :
                comfort_index = sorted(rain_index, key=lambda k: df["temperature"][rain_index][k])[(round(len(rain_index)/2)-1):(round(len(rain_index)/2)+2)]
            
            search_result = [(df.iloc[index,0],hint_weather(df.iloc[index,1],df.iloc[index,2]))  for index in comfort_index]

        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}

        else:
            if not len(search_result) == 0:
                keys = ['Recommended_time','Recommended_hint']
                Recommended_results =  [dict(zip(keys, values)) for values in search_result]
                sort_keys = ['First_recommendation','Second recommendation','Third_recommendation']
                results =  [dict(zip(sort_keys,Recommended_results))]

                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"

    if type == "浮潛":
        
        try:
            search_result = sql.execute(f"SELECT * FROM Snorkeling_three_day WHERE Observation_site='{location}' AND Instant_time>='{datetime}' AND Instant_time<'{datetime1}' ORDER BY `id` ASC")

            search_result = pd.DataFrame(search_result)
            search_result.reset_index(drop=True,inplace=True)

            remove_index=[]
            for i,tm in enumerate(search_result.iloc[:,2].apply(str)):
                if ("21:00:00" in tm) or ("00:00:00" in tm) or ("03:00:00" in tm):
                    remove_index.append(i)

            search_result.drop(index=remove_index,inplace=True)
            search_result.reset_index(drop=True,inplace=True)

            df = search_result.iloc[:,[2,5,8]]
            df.columns=["instant_time","temperature","Chance_of_rain"]
        

            df["temperature"] = df["temperature"].apply(lambda x : int(re.sub("°C","",x)))
            df["Chance_of_rain"] = df["Chance_of_rain"].apply(lambda x : int(re.sub("%","",x)))

            if np.mean(df["Chance_of_rain"]) > 90 :
                return {'status':'Not recommended','search_result':"High chance of rain is not suitable for travel"}

            rain_index = sorted(range(len(df["Chance_of_rain"])), key=lambda k: df["Chance_of_rain"][k])[:7]
            # Comfortable temperature 19~23
            if np.median(df["temperature"]) > 23:
                comfort_index = sorted(rain_index, key=lambda k: df["temperature"][rain_index][k])[:3]

            elif np.median(df["temperature"]) < 19:
                comfort_index = sorted(rain_index, key=lambda k: df["temperature"][rain_index][k],reverse=True)[:3]

            else :
                comfort_index = sorted(rain_index, key=lambda k: df["temperature"][rain_index][k])[(round(len(rain_index)/2)-1):(round(len(rain_index)/2)+2)]
            
            search_result = [(df.iloc[index,0],hint_weather(df.iloc[index,1],df.iloc[index,2]))  for index in comfort_index]

        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}

        else:
            if not len(search_result) == 0:
                keys = ['Recommended_time','Recommended_hint']
                Recommended_results =  [dict(zip(keys, values)) for values in search_result]
                sort_keys = ['First_recommendation','Second recommendation','Third_recommendation']
                results =  [dict(zip(sort_keys,Recommended_results))]

                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"


    else :
        
        try:
            search_result = sql.execute(f"SELECT * FROM Surfing_day WHERE Observation_site='{location}' AND Instant_time>='{datetime}' AND Instant_time<'{datetime1}' ORDER BY `id` ASC")

            search_result = pd.DataFrame(search_result)
            search_result.reset_index(drop=True,inplace=True)

            df = search_result.iloc[:,[2,5,8]]
            df.columns=["instant_time","temperature","Chance_of_rain"]

            remove_index=[]
            for i,tm in enumerate(search_result.iloc[:,2].apply(str)):
                if ("21:00:00" in tm) or ("00:00:00" in tm) or ("03:00:00" in tm):
                    remove_index.append(i)

            search_result.drop(index=remove_index,inplace=True)
            search_result.reset_index(drop=True,inplace=True)        

            df["temperature"] = df["temperature"].apply(lambda x : int(re.sub("°C","",x)))
            df["Chance_of_rain"] = df["Chance_of_rain"].apply(lambda x : int(re.sub("%","",x)))

            if np.mean(df["Chance_of_rain"]) > 90 :
                return {'status':'Not recommended','search_result':"High chance of rain is not suitable for travel"}


            rain_index = sorted(range(len(df["Chance_of_rain"])), key=lambda k: df["Chance_of_rain"][k])[:7]
            # Comfortable temperature 19~23
            if np.median(df["temperature"]) > 23:
                comfort_index = sorted(rain_index, key=lambda k: df["temperature"][rain_index][k])[:3]

            elif np.median(df["temperature"]) < 19:
                comfort_index = sorted(rain_index, key=lambda k: df["temperature"][rain_index][k],reverse=True)[:3]

            else :
                comfort_index = sorted(rain_index, key=lambda k: df["temperature"][rain_index][k])[(round(len(rain_index)/2)-1):(round(len(rain_index)/2)+2)]
            
            search_result = [(df.iloc[index,0],hint_weather(df.iloc[index,1],df.iloc[index,2]))  for index in comfort_index]

        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}

        else:
            if not len(search_result) == 0:
                keys = ['Recommended_time','Recommended_hint']
                Recommended_results =  [dict(zip(keys, values)) for values in search_result]
                sort_keys = ['First_recommendation','Second recommendation','Third_recommendation']
                results =  [dict(zip(sort_keys,Recommended_results))]

                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"

       


### 地圖查看 ################################### 育樂天氣預報資料(一週三天) + 日出月沒 ######################################

@app.route("/map/location/detail/1",methods=["GET"])
def show_map_detail():
    """
    育樂天氣預報資料(一週三天) + 日出月沒
    ---
    tags:
        - map
    parameters:
      - in: query
        name: type
        enum: ['海水浴場','娛樂漁業','海釣','港口','浮潛','衝浪']
        type: string
        required: true
        description: 水域種類
    responses:
      500:
        description: Fail
      200:
        description: Success
    """
    #首先獲取前端傳入的name資料

    type = request.args.get("type")

    datetime = arrow.now().shift(hours=+8).format("YYYY-MM-DD HH:mm:ss")
    datetime1 = arrow.now().shift(hours=+11).format("YYYY-MM-DD HH:mm:ss")
    datetime2 = arrow.now().shift(hours=+8).format("YYYY-MM-DD")

    sql = Database("data_contest")

    if type == "海水浴場":

        try:
            #執行sql陳述句 多說一句，f+字串的形式，可以在字串里面以{}的形式加入變數名 結果保存在result陣列中
            search_result = sql.execute(f"SELECT a.id , a.Observation_site , a.Instant_time, a.Lon , a.Lat , a.temperature , a.body_temperature , a.relative_humidity , a.Chance_of_rain , a.Maximum_wind_speed , a.Weather_phenomenon , a.Maximum_comfort_index , b.Sunrise , b.Sundown FROM (SELECT * FROM Beach_three_day WHERE Instant_time>='{datetime}' AND Instant_time<'{datetime1}') a ,Sun_moon b WHERE '{datetime2}' = b.Date_time ORDER BY `id` ASC")
            #print(pd.DataFrame(search_result))
        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}

        else:
            if not len(search_result) == 0:
                keys = ['id', 'Observation_site', 'Instant_time', 'Lon', 'Lat', 'temperature', 'body_temperature', 'relative_humidity', 'Chance_of_rain', 'Maximum_wind_speed', 'Weather_phenomenon', 'Maximum_comfort_index', 'Sunrise', 'Sundown']
                results = [dict(zip(keys, values)) for values in search_result]
                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"

    elif type == "娛樂漁業":

        try:
            search_result = sql.execute(f"SELECT a.id , a.Observation_site , a.Instant_time, a.Lon , a.Lat , a.temperature , a.body_temperature , a.relative_humidity , a.Chance_of_rain , a.Maximum_wind_speed , a.Weather_phenomenon , a.Maximum_comfort_index , b.Sunrise , b.Sundown FROM (SELECT * FROM Entertainment_three_day WHERE Instant_time>='{datetime}' AND Instant_time<'{datetime1}') a ,Sun_moon b WHERE '{datetime2}' = b.Date_time ORDER BY `id` ASC")
        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}
        else:
            if not len(search_result) == 0:
                keys = ['id', 'Observation_site', 'Instant_time', 'Lon', 'Lat', 'temperature', 'body_temperature', 'relative_humidity', 'Chance_of_rain', 'Maximum_wind_speed', 'Weather_phenomenon', 'Maximum_comfort_index', 'Sunrise', 'Sundown']
                results = [dict(zip(keys, values)) for values in search_result]
                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"

    elif type == "海釣":
    
        try:
            search_result = sql.execute(f"SELECT a.id , a.Observation_site , a.Instant_time, a.Lon , a.Lat , a.temperature , a.body_temperature , a.relative_humidity , a.Chance_of_rain , a.Maximum_wind_speed , a.Weather_phenomenon , a.Maximum_comfort_index , b.Sunrise , b.Sundown FROM (SELECT * FROM Fishing_three_day WHERE Instant_time>='{datetime}' AND Instant_time<'{datetime1}') a ,Sun_moon b WHERE '{datetime2}' = b.Date_time ORDER BY `id` ASC")
        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}
        else:
            if not len(search_result) == 0:
                keys = ['id', 'Observation_site', 'Instant_time', 'Lon', 'Lat', 'temperature', 'body_temperature', 'relative_humidity', 'Chance_of_rain', 'Maximum_wind_speed', 'Weather_phenomenon', 'Maximum_comfort_index', 'Sunrise', 'Sundown']
                results = [dict(zip(keys, values)) for values in search_result]
                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"

    elif type == "港口":
    
        try:
            search_result = sql.execute(f"SELECT a.id , a.Observation_site , a.Instant_time, a.Lon , a.Lat , a.temperature , a.body_temperature , a.relative_humidity , a.Chance_of_rain , a.Maximum_wind_speed , a.Weather_phenomenon , a.Maximum_comfort_index , b.Sunrise , b.Sundown FROM (SELECT * FROM Port_three_day WHERE Instant_time>='{datetime}' AND Instant_time<'{datetime1}') a ,Sun_moon b WHERE '{datetime2}' = b.Date_time ORDER BY `id` ASC")
        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}
        else:
            if not len(search_result) == 0:
                keys = ['id', 'Observation_site', 'Instant_time', 'Lon', 'Lat', 'temperature', 'body_temperature', 'relative_humidity', 'Chance_of_rain', 'Maximum_wind_speed', 'Weather_phenomenon', 'Maximum_comfort_index', 'Sunrise', 'Sundown']
                results = [dict(zip(keys, values)) for values in search_result]
                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"

    elif type == "浮潛":
    
        try:
            search_result = sql.execute(f"SELECT a.id , a.Observation_site , a.Instant_time, a.Lon , a.Lat , a.temperature , a.body_temperature , a.relative_humidity , a.Chance_of_rain , a.Maximum_wind_speed , a.Weather_phenomenon , a.Maximum_comfort_index , b.Sunrise , b.Sundown FROM (SELECT * FROM Snorkeling_three_day WHERE Instant_time>='{datetime}' AND Instant_time<'{datetime1}') a ,Sun_moon b WHERE '{datetime2}' = b.Date_time ORDER BY `id` ASC")
        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}
        else:
            if not len(search_result) == 0:
                keys = ['id', 'Observation_site', 'Instant_time', 'Lon', 'Lat', 'temperature', 'body_temperature', 'relative_humidity', 'Chance_of_rain', 'Maximum_wind_speed', 'Weather_phenomenon', 'Maximum_comfort_index', 'Sunrise', 'Sundown']
                results = [dict(zip(keys, values)) for values in search_result]
                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"

    else :
    
        try:
            search_result = sql.execute(f"SELECT a.id , a.Observation_site , a.Instant_time, a.Lon , a.Lat , a.temperature , a.body_temperature , a.relative_humidity , a.Chance_of_rain , a.Maximum_wind_speed , a.Weather_phenomenon , a.Maximum_comfort_index , b.Sunrise , b.Sundown FROM (SELECT * FROM Surfing_three_day WHERE Instant_time>='{datetime}' AND Instant_time<'{datetime1}') a ,Sun_moon b WHERE '{datetime2}' = b.Date_time ORDER BY `id` ASC")
        except Exception as e:
            return {'status':"error:{}".format(e), 'search_result': "code error"}
        else:
            if not len(search_result) == 0:
                keys = ['id', 'Observation_site', 'Instant_time', 'Lon', 'Lat', 'temperature', 'body_temperature', 'relative_humidity', 'Chance_of_rain', 'Maximum_wind_speed', 'Weather_phenomenon', 'Maximum_comfort_index', 'Sunrise', 'Sundown']
                results = [dict(zip(keys, values)) for values in search_result]
                return {'status':'success','search_result':results}
            else:
                return "No results were found for the data table"




### 地圖查看 ################################### 海面天氣 + 日出月沒 ######################################

@app.route("/map/location/detail/2",methods=["GET"])
def show_map_detail2():
    """
    海面天氣 + 日出月沒
    ---
    tags:
        - map
    responses:
      500:
        description: Fail
      200:
        description: Success
    """
    datetime = arrow.now().shift(hours=+8).format("YYYY-MM-DD")

    sql = Database("data_contest")

    try:
        search_result = sql.execute(f"SELECT a.id , a.Observation_site , a.Instant_date , a.Lon , a.Lat , a.Wave_conditions , a.Weather_phenomenon , b.Sunrise , b.Sundown FROM (SELECT * FROM Sea_weather WHERE Instant_date = '{datetime}') a , Sun_moon b WHERE '{datetime}' = b.Date_time ORDER BY `id` ASC")
    except Exception as e:
        return {'status':"error:{}".format(e), 'search_result': "code error"}
    else:
        if not len(search_result) == 0:
            keys = ['id', 'Observation_site', 'Instant_date' , 'Lon', 'Lat', 'Wave_conditions', 'Weather_phenomenon', 'Sunrise', 'Sundown']
            results = [dict(zip(keys, values)) for values in search_result]
            return {'status':'success','search_result':results}
        else:
            return "No results were found for the data table"




### 地圖查看 ################################### AED ######################################

@app.route("/map/AED",methods=["GET"])

def show_map_AED():
    """
    AED
    ---
    tags:
        - map
    responses:
      500:
        description: Fail
      200:
        description: Success
    """
    sql = Database("data_contest")

    try:
        search_result = sql.execute(f"SELECT id , Location_name , Lon , Lat , Location_address , Place , Weekday_time , Holiday_time FROM AED ORDER BY `id` ASC")
    except Exception as e:
        return {'status':"error:{}".format(e), 'search_result': "code error"}
    else:
        if not len(search_result) == 0:
            keys = ['id', 'Location_name', 'Lon', 'Lat', 'Location_address', 'Place', 'Weekday_time', 'Holiday_time']
            results = [dict(zip(keys, values)) for values in search_result]
            return {'status':'success','search_result':results}
        else:
            return "No results were found for the data table"



### 地圖查看 ################################### 水質 ######################################

@app.route("/map/water_quality",methods=["GET"])

def show_map_water():
    """
    水質
    ---
    tags:
        - map
    responses:
      500:
        description: Fail
      200:
        description: Success
    """
    sql = Database("data_contest")

    try:
        search_result = sql.execute(f"SELECT id , Observation_site , Lon , Lat , Data_time , Achievement_rate , Exceeded_items FROM Water_quality ORDER BY `id` ASC")
    except Exception as e:
        return {'status':"error:{}".format(e), 'search_result': "code error"}
    else:
        if not len(search_result) == 0:
            keys = ['id', 'Observation_site', 'Lon', 'Lat', 'Data_time', 'Achievement_rate', 'Exceeded_items']
            results = [dict(zip(keys, values)) for values in search_result]
            return {'status':'success','search_result':results}
        else:
            return "No results were found for the data table"


### 航線浪高 ################################### 藍色公路 ######################################

route_dict ={
    "基隆馬祖航線":"Blue_road_01",
    "臺中馬公航線":"Blue_road_02",
    "高雄馬公航線":"Blue_road_03",
    "東港小琉球航線":"Blue_road_04",
    "臺東綠島航線":"Blue_road_05",
    "布袋馬公航線":"Blue_road_06",
    "烏石龜山島航線":"Blue_road_07",
    "臺中平潭航線":"Blue_road_08",
    "臺東蘭嶼航線":"Blue_road_09",
    "後壁湖蘭嶼航線":"Blue_road_10",
    "基隆龍洞航線":"Blue_road_11",
    "龍洞龜山島航線":"Blue_road_12",
    "基隆臺中航線":"Blue_road_13",
    "基隆台州航線":"Blue_road_14",
    "臺中高雄航線":"Blue_road_15",
    "臺中金門航線":"Blue_road_16",
    "高雄花蓮航線":"Blue_road_17",
    "花蓮基隆航線":"Blue_road_18",
    "花蓮蘇澳航線":"Blue_road_19",
    "臺北港平潭航線":"Blue_road_20",
    "安平東吉島航線":"Blue_road_24",
    "蘇澳石垣島航線":"Blue_road_25",
    "基隆彭佳嶼航線":"Blue_road_26",
        }

@app.route("/sea_route",methods=["GET"])

def show_sea_route():
    """
    藍色公路 
    ---
    tags:
        - sea_route
    parameters:
      - in: query
        default: 基隆馬祖航線
        name: route
        type: string
        required: true
        description: 航線名稱
      - in: query
        default: A1
        name: route_code
        type: string
        required: true
        description: 小航線代號
      - in: query
        default: 2022/5/28 00:00
        name: datetime
        type: string
        required: true
        description: 日期
    responses:
      500:
        description: Fail
      200:
        description: Success
    """
    
    route = request.args.get("route")
    route_code = request.args.get("route_code")
    datetime = request.args.get("datetime")

    sql = Database("data_contest")    
    table_=route_dict[route] 

    try:
        search_result = sql.execute(f"SELECT id , Instant_time  , Wave_height , Wind_speed  FROM {table_} Where location_code = '{route_code}' And Instant_time >= '{datetime}' ORDER BY `id` ASC Limit 20")
    except Exception as e:
        return {'status':"error:{}".format(e), 'search_result': "code error"}
    else:
        if not len(search_result) == 0:
            keys = ['id', 'Instant_time', 'Wave_height','Wind_speed']
            results = [dict(zip(keys, values)) for values in search_result]
            return {'status':'success','search_result':results}
        else:
            return "No results were found for the data table"


if __name__ == "__main__":
    
    app.run(port=8080,debug=True)

    
  
