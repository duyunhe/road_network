# -*- coding: utf-8 -*-
# @Time    : 2018/9/4 9:53
# @Author  : 
# @简介    : 计算历史数据
# @File    : history_static.py

import cx_Oracle
import numpy as np
from datetime import datetime
from ctypes import *
from geo import bl2xy, calc_dist
from collections import defaultdict


date_weekday = {}
dll = WinDLL("../dll/CoordTransDLL.dll")


def main():
    for i in range(1, 32):
        date_weekday[i] = i % 7
    date_weekday[1] = 0
    road_speed = [[] for x in range(5432)]
    his_speed = [[] for x in range(5432)]       # 正式
    # road_speed[rid][7][24]
    for i in range(5432):
        road_speed[i] = [[] for x in range(7)]
        his_speed[i] = [[] for x in range(7)]
        for j in range(7):
            road_speed[i][j] = [[] for x in range(24)]
            his_speed[i][j] = [.0] * 24

    conn = oracle_util.get_connection()
    cursor = conn.cursor()
    sql = "select * from tb_history_speed"
    cursor.execute(sql)
    for item in cursor:
        try:
            rid = int(item[0])
            speed = float(item[1])
            hour = int(item[2])
            weekday = date_weekday[int(item[3])]
            n = int(item[4])
        except TypeError:
            print item[1]
            continue
        if rid == 0 and weekday == 0 and (hour == 0 or hour == 1):
            print speed, n, hour
        road_speed[rid][weekday][hour].append([speed, n])

    tup_list = []
    cnt = 0
    for rid in range(5432):
        for w in range(7):
            for h in range(24):
                if len(road_speed[rid][w][h]) == 0:
                    cnt += 1
                    his_speed[rid][w][h] = 0
                else:
                    ts, tn = .0, .0
                    for speed, n in road_speed[rid][w][h]:
                        ts, tn = ts + speed * n, tn + n
                    his_speed[rid][w][h] = ts / tn
            ave = np.mean(his_speed[rid][w])
            for h in range(24):
                if his_speed[rid][w][h] == 0:
                    his_speed[rid][w][h] = ave
                tup_list.append((rid, his_speed[rid][w][h], h, w))

    print cnt
    sql = "insert into tb_road_his_speed (rid, speed, data_hour, data_weekday) values(:1, :2, :3, :4)"
    cursor.executemany(sql, tup_list)
    conn.commit()
    conn.close()


def dst():
    conn = oracle_util.get_connection()
    cursor = conn.cursor()
    sql = "select * from tb_road_point order by rid, seq"
    cursor.execute(sql)
    road = [[] for x in range(5432)]
    for item in cursor:
        lng, lat = map(float, item[2:4])
        rid, seq = map(int, item[0:2])
        px, py = bl2xy(lat, lng, dll)
        road[rid].append([px, py])
    tup_list = []
    upd_sql = "update tb_road_state set road_desc = :1 where rid = :2"
    for rid in range(5432):
        last_point = None
        tot = 0
        for point in road[rid]:
            if last_point is not None:
                tot += calc_dist(last_point, point)
            last_point = point
        tup_list.append((tot, rid))
    cursor.executemany(upd_sql, tup_list)
    conn.commit()
    cursor.close()
    conn.close()


def get_rid():
    conn = cx_Oracle.connect("hz/hz@192.168.11.88/orcl")
    cursor = conn.cursor()
    sql = "select distinct(rid) from TB_ROAD_POINT_ON_MAP t"
    rid_list = {}
    cursor.execute(sql)
    for item in cursor:
        rid_list[item[0]] = 60
    conn.close()
    return rid_list


def pre():
    rid_dict = get_rid()
    conn = cx_Oracle.connect("hz/hz@192.168.11.88/orcl")
    cursor = conn.cursor()
    sql = "select * from TB_ROAD_SPEED_PRE t"
    cursor.execute(sql)
    p = defaultdict(list)
    for item in cursor:
        rid, speed = item[0:2]
        p[rid].append(speed)

    tup_list = []
    for rid, spd_list in p.iteritems():
        spd_list.sort()
        if len(spd_list) < 5:
            ave = np.mean(spd_list)
        else:
            ave = np.mean(spd_list[2:-2])
        rid_dict[rid] = min(100.0, float(ave))
        # tup_list.append((rid, rid_dict[rid], '1'))
    for rid, spd in rid_dict.iteritems():
        tup_list.append((rid, spd, '1'))
    ins_sql = "insert into tb_road_def_speed values(:1, :2, :3)"
    cursor.executemany(ins_sql, tup_list)
    conn.commit()
    cursor.close()
    conn.close()


# pre()
