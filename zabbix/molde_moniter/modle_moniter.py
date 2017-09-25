#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Last modified: dujiaixn (fapple1@sina.com)

import time, datetime
import os
from json import loads

import pymysql

# hadoop fs -get /daiqiang/sticker_model_monitor/20170911/* ./result

# 获取时间区间
now_day = datetime.datetime.now().strftime("%Y%m%d")
now_hour = datetime.datetime.now().strftime("%Y%m%d%H")
timeArray = time.strptime(now_hour, "%Y%m%d%H")
st_end = int(time.mktime(timeArray)) * 1000
st_star = st_end - 3600 * 1000  # 一个小时粒度
now_time = (datetime.datetime.now() - datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")

log_tmp = "/home/dujiaxin/model_moniter/tmp"

db = pymysql.connect(
    host="172.31.21.163",
    user="monitor_user",
    password="uXU9YaSZYn",
    db="kika_tableau",
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)
cur = db.cursor()

# 拉取日志
os.popen("hadoop fs -get /daiqiang/sticker_model_monitor/" + now_day + "/* " + log_tmp).read()

# 获取指定区间内
file_names = os.popen("ls " + log_tmp).read()
file_names = file_names.split("\n")
file_names = [x for x in file_names if x != '' and st_end >= int(x) >= st_star]

ask_count = 0
user_count = set()
# 轮询日志
for dir_name in file_names:
    with open(log_tmp + "/" + dir_name + "/part-00000", "r", encoding="utf-8") as f:
        for line in f:
            ask_count += 1
            line_json = loads(line)
            user_count.add(line_json["duid"])


os.popen("rm -rf " + log_tmp + "/*").read()

sql_model = """
insert into api_request
    (project_name, name, condition1, value, start_time, start_timestamp, end_time, create_time)
    values('model_moniter' ,'ikey' ,'{}' ,'{}' ,'{}' ,'{}' ,'{}' ,'{}')
"""

# print(sql_model.format("ask_count", ask_count, now_hour, st_end, now_hour, now_time, ))
# print(sql_model.format("user_count", len(user_count), now_hour, st_end, now_hour, now_time, ))
cur.execute(sql_model.format("ask_count", ask_count, now_hour, st_end, now_hour, now_time, ))
cur.execute(sql_model.format("user_count", len(user_count), now_hour, st_end, now_hour, now_time, ))
db.commit()

if __name__ == '__main__':
    print(now_day)
