#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Last modified: dujiaixn (fapple1@sina.com)

import time
import datetime

import boto3
import pymysql

db = pymysql.connect(
    host="172.31.21.163",
    user="monitor_user",
    password="uXU9YaSZYn",
    db="kika_tableau",
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)
cur = db.cursor()

file_path = "/home/datauser/data-monitor/log_reader/tmp.txt"
app_key = "e2934742f9d3b8ef2b59806a041ab389"
result = dict()
log_name = (datetime.datetime.now() - datetime.timedelta(hours=2)).strftime("%Y%m%d%H")
now_time = (datetime.datetime.now() - datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
ignore_sign = {'', '$'}

s3_pathes = {
    "nginx1": "nginx-log/api.kikakeyboard.com-success.log.nginx1." + log_name,
    "nginx2": "nginx-log/api.kikakeyboard.com-success.log.nginx2." + log_name
}
# "nginx-log/api.kikakeyboard.com-success.log.nginx1.2017091209",

s3 = boto3.resource('s3')


def count_user(s3_path, nginx):
    s3.Bucket('xinmei-backend-log').download_file(
        s3_path,
        file_path
    )

    with open(file_path) as f:
        for l in f:
            try:
                msg = l.split(" ")
                duid, appkey = msg[13][1:-1].split("/")
                if appkey == app_key:
                    country = msg[14].split("/")[1]
                    language = msg[15].split("/")[1]
                    if country in ignore_sign or language in ignore_sign:
                        continue
                    st = time.strptime(msg[3].split(" ")[0][1:], "%d/%b/%Y:%H:%M:%S")
                    r = result.setdefault((country, language), {"time": st, "users": set()})
                    r["users"].add(duid)
            except Exception as e:
                pass


# 下载所有日志
for ng in s3_pathes:
    count_user(s3_pathes[ng], ng)

for x in result:
    country, language = x
    sql = """
    insert into api_request
    (project_name, name, condition3, condition1, condition2, value, start_time, start_timestamp, end_time, create_time)
    values('{}' ,'{}' ,'{}' ,'{}' ,'{}' ,'{}' ,'{}' ,'{}' ,'{}' ,'{}')
    """.format("data-monitor",
               "nginx_all",
               "user_only",
               country,
               language,
               len(result[x]["users"]),
               time.strftime("%Y%m%d%H", result[x]["time"]),
               int(time.mktime(result[x]["time"])),
               time.strftime("%Y%m%d%H", result[x]["time"]),
               now_time,
               )
    cur.execute(sql)
db.commit()
