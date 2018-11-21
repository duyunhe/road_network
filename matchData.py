# -*- coding: utf-8 -*-
# @Time    : 2018/8/13 16:22
# @Author  : 
# @简介    : 计算历史数据
# @File    : matchData.py

import cx_Oracle
from datetime import datetime, timedelta
from geo import bl2xy, calc_dist
import matplotlib.pyplot as plt
from time import clock
import numpy as np
import json
import redis
from map_matching import MapMatching
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


def debug_time(func):
    def wrapper(*args, **kwargs):
        bt = clock()
        a = func(*args, **kwargs)
        et = clock()
        print "read_data.py", func.__name__, "cost", round(et - bt, 4), "secs"
        return a
    return wrapper

mm = MapMatching()
data_list, edge_list, point_list = {}, {}, {}


class TaxiData:
    def __init__(self, veh, px, py, stime, state, speed, car_state, direction):
        self.veh = veh
        self.px, self.py, self.stime, self.state, self.speed = px, py, stime, state, speed
        self.stop_index, self.dist, self.car_state, self.direction = 0, 0, car_state, direction
        self.angle = 0

    def set_index(self, index):
        self.stop_index = index

    def set_angle(self, angle):
        self.angle = angle


def cmp1(data1, data2):
    if data1.stime > data2.stime:
        return 1
    elif data1.stime < data2.stime:
        return -1
    else:
        return 0


def get_history_speed(conn, speed_time):
    sql = "select * from tb_history_speed where data_hour = {0} and data_weekday = {1}".format(
        speed_time.hour, speed_time.weekday()
    )
    cursor = conn.cursor()
    cursor.execute(sql)
    speed = 0.0
    for item in cursor:
        speed = float(item[1])
    return speed


@debug_time
def get_all_gps_data():
    """
    从数据库中获取一段时间的GPS数据
    :return: 
    """
    end_time = datetime(2018, 5, 15, 14, 0, 0)
    conn = cx_Oracle.connect('hz/hz@192.168.11.88:1521/orcl')
    begin_time = end_time + timedelta(minutes=-10)
    sql = "select px, py, speed_time, state, speed, carstate, direction, vehicle_num from " \
          "TB_GPS_1805 t where speed_time >= :1 " \
          "and speed_time < :2 and vehicle_num = '浙AT3069' order by speed_time "

    # sql = "select px, py, speed_time, state, speed, carstate, direction, vehicle_num from " \
    #       "TB_GPS_1805 t where speed_time >= :1 and speed_time < :2"

    tup = (begin_time, end_time)
    cursor = conn.cursor()
    cursor.execute(sql, tup)
    veh_trace = {}
    static_num = {}
    for item in cursor.fetchall():
        lng, lat = map(float, item[0:2])
        if 119 < lng < 121 and 29 < lat < 31:
            px, py = bl2xy(lat, lng)
            state = int(item[3])
            stime = item[2]
            speed = float(item[4])
            car_state = int(item[5])
            ort = float(item[6])
            veh = item[7][-6:]
            # if veh != 'AT0956':
            #     continue
            taxi_data = TaxiData(veh, px, py, stime, state, speed, car_state, ort)
            try:
                veh_trace[veh].append(taxi_data)
            except KeyError:
                veh_trace[veh] = [taxi_data]
            try:
                static_num[veh] += 1
            except KeyError:
                static_num[veh] = 1
    new_dict = {}
    record_cnt = 0
    for veh, trace in veh_trace.iteritems():
        trace.sort(cmp1)
        new_trace = []
        last_data = None
        for data in trace:
            esti = True
            dist = 0
            if last_data is not None:
                dist = calc_dist([data.px, data.py], [last_data.px, last_data.py])
                dt = (data.stime - last_data.stime).total_seconds()
                if data.state == 0:
                    esti = False
                # 过滤异常
                if dt <= 10 or dt > 180:
                    esti = False
                elif dist > 100 / 3.6 * dt:  # 距离阈值
                    esti = False
                elif data.car_state == 1:  # 非精确
                    esti = False
                elif data.speed == last_data.speed and data.direction == last_data.direction:
                    esti = False
                elif dist < 10:             # GPS的误差在10米，不准确
                    esti = False
            last_data = data
            if esti:
                new_trace.append(data)
                # print i, dist
                # i += 1
        if len(new_trace) >= 5:
            new_dict[veh] = new_trace
            record_cnt += len(new_trace)
    print "all car records", record_cnt, "car number", len(new_dict)
    cursor.close()
    conn.close()
    return new_dict


@debug_time
def get_gps_data_from_redis():
    bt = clock()
    conn = redis.Redis(host="192.168.11.229", port=6300, db=1)
    keys = conn.keys()
    new_trace = {}
    if len(keys) != 0:
        m_res = conn.mget(keys)
        et = clock()
        veh_trace = {}
        static_num = {}
        for data in m_res:
            try:
                js_data = json.loads(data)
                lng, lat = js_data['longi'], js_data['lati']
                veh, str_time = js_data['isu'], js_data['speed_time']
                speed = js_data['speed']
                stime = datetime.strptime(str_time, "%Y-%m-%d %H:%M:%S")
                state = 1
                car_state = 0
                ort = js_data['ort']
                if 119 < lng < 121 and 29 < lat < 31:
                    px, py = bl2xy(lat, lng)
                    taxi_data = TaxiData(veh, px, py, stime, state, speed, car_state, ort)
                    try:
                        veh_trace[veh].append(taxi_data)
                    except KeyError:
                        veh_trace[veh] = [taxi_data]
                    try:
                        static_num[veh] += 1
                    except KeyError:
                        static_num[veh] = 1
            except TypeError:
                pass
        print "redis cost ", et - bt
        cnt = {}
        for veh, trace in veh_trace.iteritems():
            try:
                cnt[len(trace)] += 1
            except KeyError:
                cnt[len(trace)] = 1
        print cnt

        for veh, trace in veh_trace.iteritems():
            trace.sort(cmp1)
            filter_trace = []
            last_data = None
            for data in trace:
                esti = True
                if last_data is not None:
                    dist = calc_dist([data.px, data.py], [last_data.px, last_data.py])
                    dt = (data.stime - last_data.stime).total_seconds()
                    if data.state == 0:
                        esti = False
                    # 过滤异常
                    if dt <= 10 or dt > 180:
                        esti = False
                    elif dist > 100 / 3.6 * dt:  # 距离阈值
                        esti = False
                    elif data.car_state == 1:  # 非精确
                        esti = False
                    # elif data.speed == last_data.speed and data.direction == last_data.direction:
                    #     esti = False
                    elif dist < 10:  # GPS的误差在10米，不准确
                        esti = False
                last_data = data
                if esti:
                    filter_trace.append(data)
            if 4 <= len(filter_trace) <= 40:
                new_trace[veh] = filter_trace

        # cnt static
        cnt = {}
        for veh, trace in new_trace.iteritems():
            try:
                cnt[len(trace)] += 1
            except KeyError:
                cnt[len(trace)] = 1
            # if len(trace) == 13:
            #     print veh

    # print "get all gps data {0}".format(len(veh_trace))
    # print "all car:{0}, ave:{1}".format(len(static_num), len(trace) / len(static_num))
    return new_trace


def get_gps_data():
    end_time = datetime(2018, 5, 8, 15, 0, 0)
    conn = cx_Oracle.connect('hz/hz@192.168.11.88:1521/orcl')
    bt = clock()
    begin_time = end_time + timedelta(minutes=-5)
    sql = "select px, py, speed_time, state, speed, carstate, direction, vehicle_num from " \
          "TB_GPS_1805 t where speed_time >= :1 " \
          "and speed_time < :2 and vehicle_num = '浙ATE638' order by speed_time "

    # sql = "select px, py, speed_time, state, speed, carstate, direction, vehicle_num from " \
    #       "TB_GPS_1805 t where speed_time >= :1 and speed_time < :2"

    tup = (begin_time, end_time)
    cursor = conn.cursor()
    cursor.execute(sql, tup)
    veh_trace = {}
    static_num = {}
    for item in cursor.fetchall():
        lng, lat = map(float, item[0:2])
        if 119 < lng < 121 and 29 < lat < 31:
            px, py = bl2xy(lat, lng)
            state = int(item[3])
            stime = item[2]
            speed = float(item[4])
            car_state = int(item[5])
            ort = float(item[6])
            veh = item[7][-6:]
            # if veh != 'AT0956':
            #     continue
            taxi_data = TaxiData(veh, px, py, stime, state, speed, car_state, ort)
            try:
                veh_trace[veh].append(taxi_data)
            except KeyError:
                veh_trace[veh] = [taxi_data]
            try:
                static_num[veh] += 1
            except KeyError:
                static_num[veh] = 1
    et = clock()
    new_dict = {}
    for veh, trace in veh_trace.iteritems():
        trace.sort(cmp1)
        new_trace = []
        last_data = None
        i = 0
        for data in trace:
            esti = True
            dist = 0
            if last_data is not None:
                dist = calc_dist([data.px, data.py], [last_data.px, last_data.py])
                dt = (data.stime - last_data.stime).total_seconds()
                if data.state == 0:
                    esti = False
                # 过滤异常
                if dt <= 10 or dt > 180:
                    esti = False
                elif dist > 100 / 3.6 * dt:  # 距离阈值
                    esti = False
                elif data.car_state == 1:  # 非精确
                    esti = False
                elif data.speed == last_data.speed and data.direction == last_data.direction:
                    esti = False
                elif dist < 20:  # GPS的误差在10米，不准确
                    esti = False
            last_data = data
            if esti:
                new_trace.append(data)
                # print i, dist
                # i += 1
        new_dict[veh] = new_trace
    print "get all gps data", et - bt
    # print "all car:{0}, ave:{1}".format(len(static_num), len(trace) / len(static_num))
    cursor.close()
    conn.close()
    return new_dict


def draw_trace(trace):
    x, y = [], []
    for i, data in enumerate(trace):
        plt.text(data.px + 5, data.py + 5, "{0}".format(i))
        x.append(data.px)
        y.append(data.py)
    # minx, maxx, miny, maxy = min(x), max(x), min(y), max(y)
    # plt.xlim(minx, maxx)
    # plt.ylim(miny, maxy)
    plt.plot(x, y, 'k+')


def draw_points(pt_list):
    if len(pt_list) == 0:
        return
    x, y = zip(*pt_list)
    plt.plot(x, y, 'b+')
    for i in range(len(pt_list)):
        x, y = pt_list[i][0:2]


def save_road_speed(conn, road_speed):
    sql = "delete from tb_road_speed"
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    sql = "insert into tb_road_speed values(:1, :2, :3, :4, :5, 0)"
    tup_list = []
    for rid, speed_list in road_speed.iteritems():
        speed, num, tti = speed_list[:]
        if speed == float('nan') or speed == float('inf'):
            print rid, speed_list
            continue
        dt = datetime.now()
        tup = (rid, float('%.2f' % speed), num, tti, dt)
        try:
            cursor.execute(sql, tup)
        except cx_Oracle.DatabaseError:
            print tup
        # tup_list.append(tup)
    # cursor.executemany(sql, tup_list)
    conn.commit()
    print "road speed updated!"


def get_def_speed(conn):
    sql = "select rid, speed from tb_road_def_speed where map_level = 1"
    cursor = conn.cursor()
    cursor.execute(sql)
    def_speed = {}
    for item in cursor:
        rid = int(item[0])
        spd = float(item[1])
        def_speed[rid] = spd
    return def_speed


def save_roadspeed_bak(conn, speed_dict):
    now = datetime.now()
    cursor = conn.cursor()
    sql = "insert into tb_road_speed_bak values(:1, :2, :3, :4, to_date(:5, 'yyyy-mm-dd hh24:mi:ss'))"
    tup_list = []
    for rid, speed_list in speed_dict.iteritems():
        speed, num, tti = speed_list[:]
        if speed == float('nan') or speed == float('inf'):
            continue
        DT = now.strftime("%Y-%m-%d %H:%M:%S")
        tup = (rid, speed, num, tti, DT)
        tup_list.append(tup)
    cursor.executemany(sql, tup_list)
    conn.commit()


def match2road(veh, data, cnt):
    """
    :param veh: 车辆id
    :param data: TaxiData，见map_matching
    :param cnt:  for debug
    :return: point, edge, speed_list[edge, edge_speed], state
    """
    try:
        last_data, last_edge, last_point = data_list[veh], edge_list[veh], point_list[veh]
        # dist = calc_dist([data.px, data.py], [last_data.px, last_data.py])
    except KeyError:
        last_data, last_edge, last_point, dist = None, None, None, None

    # if veh == 'AT3006':
    #     print "data", cnt, data.speed, data.stime, dist
    cur_point, cur_edge = mm.PNT_MATCH(data, last_data, last_edge, cnt)
    # print "process {0}".format(cnt)
    speed_list = []
    ret = -1
    esti = True

    if cnt == 0:
        esti = False
    if not esti:
        point_list[veh], edge_list[veh] = cur_point, cur_edge
        data_list[veh] = data
        return cur_point, cur_edge, speed_list, ret

    # # 两种情况
    # if last_edge is not None and cur_edge is not None:
    #     trace, speed_list = estimate_road_speed(last_edge, cur_edge, last_point,
    #                                             cur_point, last_data, data, cnt)
    #     # for edge, spd in speed_list:
    #     #     if edge.way_id == 2839:
    #     #         print 'suc', veh, spd, data.stime
    #     ret = 0
    # elif last_edge is None and cur_edge is not None:
    #     speed_list = [[cur_edge, data.speed]]
    #     # for edge, spd in speed_list:
    #     #     if edge.way_id == 2839:
    #     #         print 'first', veh, spd, data.stime
    #     ret = 1

    point_list[veh], edge_list[veh] = cur_point, cur_edge
    data_list[veh] = data
    return cur_point, cur_edge, speed_list, ret


@debug_time
def main():
    trace_dict = get_all_gps_data()
    # conn = oracle_util.get_connection()
    # trace_dict = get_gps_data()
    # trace_dict = get_gps_data_from_redis()
    # return
    # print len(trace)
    mod_list = []
    dtrace = []
    tup_list = []
    for veh, trace in trace_dict.iteritems():
        # if veh != u'o浙A3086E':
        #     continue
        for i, data in enumerate(trace):
            mod_point, cur_edge, speed_list, ret = match2road(data.veh, data, i)
            # for edge, spd in speed_list:
            #     try:
            #         road_temp[edge.way_id].append([spd, edge.edge_length])
            #     except KeyError:
            #         road_temp[edge.way_id] = [[spd, edge.edge_length]]
            #     tup = (edge.way_id, spd, veh, data.stime)
            #     tup_list.append(tup)
            dtrace = trace
            if mod_point is not None:
                mod_list.append(mod_point)
        # break

    # print "update speed detail"
    # def_speed = get_def_speed(conn)
    # road_speed = {}
    # # his_speed_list = get_history_speed_list(conn, datetime.now())
    # for rid, sp_list in road_temp.iteritems():
    #     W, S = 0, 0
    #     for sp, w in sp_list:
    #         S, W = S + sp * w, W + w
    #     spd = S / W
    #     n_sample = len(sp_list)
    #     # 当采样点过少时，假设道路通畅，用自由流速度加权
    #     if n_sample < 10:
    #         # weight = 10 - n_sample ?
    #         spd = (spd * n_sample + def_speed[rid] * (10 - n_sample)) / 10
    #     # radio = def_speed[rid] / spd
    #     idx = get_tti_v0(spd, def_speed[rid])
    #     # print rid, S / W, len(sp_list), radio, idx
    #     road_speed[rid] = [spd, n_sample, idx]
    # # 直接上历史记录
    # for rid in range(ROAD_NUMBER):
    #     if rid not in road_speed.keys():
    #         idx = get_tti_v2(def_speed[rid], def_speed[rid])
    #         road_speed[rid] = [def_speed[rid], 0, idx]
    # save_road_speed(conn, road_speed)
    # # save_roadspeed_bak(conn, road_speed)
    # print estimate_speed.normal_cnt, estimate_speed.ab_cnt, estimate_speed.error_cnt

    # print "main process {0}".format(len(trace_dict)), et - bt
    # draw_trace(dtrace)
    # draw_points(mod_list)
    # mm.plot_map({})
    # draw_map(road_speed)
    # plt.show()
    # conn.close()


# get_gps_data_from_redis()
if __name__ == '__main__':
    main()
