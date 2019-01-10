# -*- coding: utf-8 -*-
# @Time    : 2018/11/19 11:14
# @Author  : yhdu@tongwoo.cn
# @简介    : 
# @File    : read_data.py


import sqlite3
import cx_Oracle
import matplotlib.pyplot as plt
from geo import bl2xy, dog_last
from map_struct import Road, Point
import math
from math import pi
from time import clock


def debug_time(func):
    def wrapper(*args, **kwargs):
        bt = clock()
        a = func(*args, **kwargs)
        et = clock()
        print "read_data.py", func.__name__, "cost", round(et - bt, 2), "secs"
        return a
    return wrapper


def main():
    road_list = load_sqlite_road()
    for road in road_list:
        x_list, y_list = [], []
        for pt in road.point_list:
            x, y = pt.px, pt.py
            x_list.append(x)
            y_list.append(y)
        plt.plot(x_list, y_list)
    plt.show()


def remove():
    conn = sqlite3.connect('./data/hz.db3')
    cursor = conn.cursor()
    fp = open('./data/rm.txt')
    for line in fp.readlines():
        item = int(line.strip())
        sql = "delete from tb_seg_point where s_id = :1"
        cursor.execute(sql, (item,))
        # ok
    conn.commit()
    conn.close()


def road_simplify(road):
    """
    简化
    :param road: Road
    :return:
    """
    pts = road.point_list
    xy_list = []
    for pt in pts:
        xy_list.append([pt.px, pt.py])
    xy_list = dog_last(xy_list)
    road.point_list = []
    for xy in xy_list:
        pt = Point(xy[0], xy[1])
        road.add_point(pt)
    road.gene_segment()


@debug_time
def load_oracle_road():
    """
    从原始做好的oracle路网中读取路网 
    :return: list of Road
    """
    conn = cx_Oracle.connect("hz/hz@192.168.11.88/orcl")
    cursor = conn.cursor()
    cursor.execute("select sp.s_id, sp.seq, sp.longti, sp.lati, s.s_name, s.direction"
                   " from tb_seg_point sp, tb_segment s " 
                   "where sp.s_id = s.s_id and s.rank != '次要道路' and s.rank != '连杆道路'"
                   " order by s_id, seq")
    xy_dict = {}
    name_dic = {}           # name
    dir_dict = {}           # direction
    for items in cursor:
        rid = int(items[0])
        l, b = map(float, items[2:4])
        l, b = wgs84togcj02(l, b)
        x, y = bl2xy(b, l)
        pt = Point(x, y)
        name = items[4]
        ort = int(items[5])
        try:
            xy_dict[rid].append(pt)
        except KeyError:
            xy_dict[rid] = [pt]
            name_dic[rid] = name
            dir_dict[rid] = ort
    road_list = []
    # cnt0, cnt1 = 0, 0
    for rid, items in xy_dict.iteritems():
        ort = dir_dict[rid]
        if ort == 2:
            items.reverse()
        r = Road(name_dic[rid], ort, rid)
        r.set_point_list(items)
        # cnt0 += len(r.point_list)
        road_list.append(r)
        road_simplify(r)
        # cnt1 += len(r.point_list)
    cursor.close()
    conn.close()
    return road_list


@debug_time
def load_sqlite_road():
    """
    从sqlite3中读取路网
    :return: list of Road
    """
    conn = sqlite3.connect('./data/hz.db3')
    cursor = conn.cursor()
    cursor.execute("select s_id, seq, longti, lati from tb_seg_point order by s_id, seq")
    xy_dict = {}
    for items in cursor:
        rid = int(items[0])
        l, b = map(float, items[2:4])
        l, b = wgs84togcj02(l, b)
        x, y = bl2xy(b, l)
        pt = Point(x, y)
        try:
            xy_dict[rid].append(pt)
        except KeyError:
            xy_dict[rid] = [pt]
    road_list = []
    # cnt0, cnt1 = 0, 0
    for rid, items in xy_dict.iteritems():
        r = Road("", 0, rid)
        r.set_point_list(items)
        # cnt0 += len(r.point_list)
        road_list.append(r)
        road_simplify(r)
        # cnt1 += len(r.point_list)
    cursor.close()
    conn.close()
    return road_list


def transformlat(lng, lat):
    ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + \
        0.1 * lng * lat + 0.2 * math.sqrt(math.fabs(lng))
    ret += (20.0 * math.sin(6.0 * lng * pi) + 20.0 *
            math.sin(2.0 * lng * pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lat * pi) + 40.0 *
            math.sin(lat / 3.0 * pi)) * 2.0 / 3.0
    ret += (160.0 * math.sin(lat / 12.0 * pi) + 320 *
            math.sin(lat * pi / 30.0)) * 2.0 / 3.0
    return ret


def transformlng(lng, lat):
    ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + \
        0.1 * lng * lat + 0.1 * math.sqrt(math.fabs(lng))
    ret += (20.0 * math.sin(6.0 * lng * pi) + 20.0 *
            math.sin(2.0 * lng * pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lng * pi) + 40.0 *
            math.sin(lng / 3.0 * pi)) * 2.0 / 3.0
    ret += (150.0 * math.sin(lng / 12.0 * pi) + 300.0 *
            math.sin(lng / 30.0 * pi)) * 2.0 / 3.0
    return ret


def wgs84togcj02(lng, lat):
    """
    WGS84转GCJ02(火星坐标系)
    :param lng:WGS84坐标系的经度
    :param lat:WGS84坐标系的纬度
    :return: lng, lat
    """
    a = 6378245.0  # 长半轴
    ee = 0.00669342162296594323  # 扁率

    dlat = transformlat(lng - 105.0, lat - 35.0)
    dlng = transformlng(lng - 105.0, lat - 35.0)

    radlat = lat / 180.0 * pi
    magic = math.sin(radlat)
    magic = 1 - ee * magic * magic
    sqrtmagic = math.sqrt(magic)
    dlat = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrtmagic) * pi)
    dlng = (dlng * 180.0) / (a / sqrtmagic * math.cos(radlat) * pi)
    mglat = lat + dlat
    mglng = lng + dlng

    return [mglng, mglat]

