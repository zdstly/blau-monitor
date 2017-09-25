#!/usr/bin/env python
# vim: set fileencoding=UTF-8 :
# -*- coding: utf-8 -*-
# Last modified: dujiaixn (fapple1@sina.com)
import time
import sys

import pymysql

db = pymysql.connect(
                host="172.31.21.163",
                user="monitor_user",
                password="uXU9YaSZYn",
                db="koala_results",
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
cur = db.cursor()

result = dict()
key_action = []
app_key = sys.argv[1]
the_time = time.strftime('%Y%m%d',time.localtime(time.time() - 24*60*60))

#cur.execute("select * from event_whole where cdate={}".format(time.strftime('%Y%m%d',time.localtime(time.time() - 2*24*60*60) )))
cur.execute("select * from event_whole where cdate='{}' and app_key='{}'".format(the_time,  app_key))
for pu in cur.fetchall():
    r = result.setdefault(pu["app_key"], {"p": 0, "u": 0})
    if pu['item'] == 'send' and pu['layout'] == 'keyboard_sticker2_suggestion_pop':
        r["p"] += int(pu["pcount"])
    if pu['item'] == 'pop' and pu['layout'] == 'sticker2_suggestion':
        r["u"] += int(pu["ucount"])
for x in result:
    print("{} {} {} {} {}".format(x, result[x]["p"], result[x]["u"], "0", "0"))
