python == 3.8.10

instant_data 每小時抓一次 
threeday_data 每6小時抓一次
oneweak_data 每6小時抓一次
sea_data 每6小時抓一次
blue_road 每6小時抓一次
(建議xx:20:00)

push_msg.py
每日推播
針對有開啟推播功能的使用者，推播當日地區的天氣狀況
排程：每日早上8點執行

push_tips.py
緊急推播
如果所設定的推播地點的舒適度，由溫度及濕度計算，舒適度過高(太炎熱)或過低(太寒冷)、累積雨量達豪雨標準就會推播警示訊息
排程：每6個小時跑一次
