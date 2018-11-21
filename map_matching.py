# -*- coding: utf-8 -*-
# @Time    : 2018/8/13 10:03
# @Author  : 
# @简介    : 地图匹配模块
# @File    : map_matching.py

from map_struct import MapEdge, MapNode, MapRoad, DistNode
from geo import point2segment, point_project, calc_included_angle, calc_dist, point_project_edge
import matplotlib.pyplot as plt
from read_data import load_sqlite_road
import math
from time import clock
import Queue
from sklearn.neighbors import KDTree
import numpy as np


def debug_time(func):
    def wrapper(*args, **kwargs):
        bt = clock()
        a = func(*args, **kwargs)
        et = clock()
        print "map_matching function", func.__name__, "cost", round(et - bt, 2), "secs"
        return a
    return wrapper


class MapInfo:
    """
    保存地图信息
    """
    def __new__(cls):
        # 单例。每一次实例化的时候，只会返回同一个instance对象
        if not hasattr(cls, 'instance'):
            cls.instance = super(MapInfo, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self.map_node = {}  # 地图节点MapNode
        self.map_edge = []  # 地图边，每一线段都是一条MapEdge，MapEdge两端是MapNode
        self.map_road = {}  # 记录道路信息
        self.kdt, self.X = None, None

    def load_map(self):
        print "===========load map==========="
        print "... ..."
        road_list = load_sqlite_road()
        map_temp_node = {}  # 维护地图节点
        road_point = {}

        for road in road_list:
            rid = road.rid
            r = MapRoad("", 0, 0)
            self.map_road[rid] = r
            pt_list = []
            for point in road.point_list:
                pt_list.append([point.px, point.py])
            road_point[rid] = pt_list

        # 维护节点dict
        # 每一个点都单独列入dict，边用两点表示，道路用若干点表示
        for rid, xylist in road_point.iteritems():
            r = self.map_road[rid]
            last_nodeid = -1
            lastx, lasty = None, None
            for i, xy in enumerate(xylist):
                x, y = round(xy[0], 2), round(xy[1], 2)
                str_bl = "{0},{1}".format(x, y)
                # x, y = bl2xy(lat, lng)
                try:
                    nodeid = map_temp_node[str_bl]
                    nd = self.map_node[nodeid]
                except KeyError:
                    nodeid = len(map_temp_node)
                    map_temp_node[str_bl] = nodeid
                    nd = MapNode([x, y], nodeid)
                    self.map_node[nodeid] = nd
                if i > 0:
                    edge_length = calc_dist([x, y], [lastx, lasty])
                    edge = MapEdge(self.map_node[last_nodeid], self.map_node[nodeid], False,
                                   len(self.map_edge), edge_length, rid)
                    self.map_edge.append(edge)
                r.add_node(nd)
                last_nodeid, lastx, lasty = nodeid, x, y
        print "read", len(self.map_road), "roads"
        print "load", len(self.map_node), "nodes", len(self.map_edge), "edges"
        print "=================================="

    def store_link(self):
        for edge in self.map_edge:
            n0, n1 = edge.node0, edge.node1
            if edge.oneway is True:
                n0.add_link(edge, n1)
                n1.add_rlink(edge, n0)
            else:
                n0.add_link(edge, n1)
                n1.add_link(edge, n0)
                n0.add_rlink(edge, n1)
                n1.add_rlink(edge, n0)

    def init_map(self):
        self.load_map()
        self.store_link()

    def get_node(self):
        return self.map_node

    def get_edge(self):
        return self.map_edge


class MapMatching(object):
    """
    封装地图匹配类
    在init时调用MapInfo.init_map，读取地图及初始化数据结构
    单点匹配时调用LCL_MATCH，传入15-20个GPS点的位置和车辆标识，得到轨迹的匹配点和匹配边
    """

    @debug_time
    def __init__(self):
        print "map matching init..."
        self.mi = MapInfo()
        self.mi.init_map()
        self.map_node = self.mi.get_node()
        self.map_edge = self.mi.get_edge()
        self.nodeid_list = []
        self.kdt, self.X = self.make_kdtree()
        self.last_edge_list, self.mod_list = {}, {}     # 记录每辆车之前的点
        self.state_list = {}                            # 记录上一个状态
        print "map matching ready"

    def make_kdtree(self):
        """
        对node建立kdtree
        :return: 
        """
        nd_list = []
        for key, item in self.mi.map_node.items():
            self.nodeid_list.append(key)
            nd_list.append(item.point)
        X = np.array(nd_list)
        return KDTree(X, leaf_size=2, metric="euclidean"), X

    def plot_map(self, road_speed):
        """
        根据路况绘制地图
        :param road_speed: {rid:speed_list}
        speed_list :[spd, sample_number, tti]
        :return: 
        """
        for rid, road in self.mi.map_road.iteritems():
            node_list = road.node_list
            x, y = [], []
            for node in node_list:
                x.append(node.point[0])
                y.append(node.point[1])
            try:
                speed, _, tti = road_speed[rid]
                if tti > 8:
                    c = 'm'
                elif 6 < tti <= 8:
                    c = 'red'
                elif 4 < tti <= 6:
                    c = 'darkorange'
                elif 2 < tti <= 4:
                    c = 'green'
                else:
                    c = 'green'
            except KeyError:
                c = 'k'
            plt.plot(x, y, c=c, alpha=0.3, linewidth=2)

            # if c == 'm' or c == 'red':
            #     plt.text((x[0] + x[-1]) / 2, (y[0] + y[-1]) / 2, "{0}".format(rid))
            # plt.text(x[0], y[0], "{0},{1}".format(rid, speed))

        # for e in self.mi.map_edge:
        #     if e.edge_index == 370 or e.edge_index == 371:
        #         draw_edge(e, 'g')
        plt.show()

    def init_candidate_queue(self, last_point, last_edge, can_queue, node_set):
        """
        initialize the queue, add one or two points of the last edge
        """
        _, ac, state = point_project_edge(last_point, last_edge)
        project_dist = np.linalg.norm(np.array(ac))
        dist0, dist1 = project_dist, last_edge.edge_length - project_dist
        if dist0 > last_edge.edge_length:
            dist0, dist1 = last_edge.edge_length, 0

        if last_edge.oneway:
            node = last_edge.node1
            dnode = DistNode(node, dist1)
            can_queue.put(dnode)
        else:
            node = last_edge.node0
            dnode = DistNode(node, dist0)
            can_queue.put(dnode)
            node_set.add(node.nodeid)

            node = last_edge.node1
            dnode = DistNode(node, dist1)
            can_queue.put(dnode)

        node_set.add(node.nodeid)

    def get_candidate_first(self, taxi_data, kdt, X):
        """            
        get candidate edges from road network which fit point 
        :param taxi_data: Taxi_Data  .px, .py, .speed, .stime
        :param kdt: kd-tree
        :return: edge candidate list  list[edge0, edge1, edge...]
        """
        dist, ind = kdt.query([[taxi_data.px, taxi_data.py]], k=15)

        pts = []
        seg_set = set()
        # fetch nearest map nodes in network around point, then check their linked edges
        for i in ind[0]:
            pts.append([X[i][0], X[i][1]])
            node_id = self.nodeid_list[i]
            edge_list = self.map_node[node_id].link_list
            for e, nd in edge_list:
                seg_set.add(e.edge_index)
            # here, need reverse link,
            # for its first node can be far far away, then this edge will not be included
            edge_list = self.map_node[node_id].rlink_list
            for e, nd in edge_list:
                seg_set.add(e.edge_index)

        edge_can_list = []
        for i in seg_set:
            edge_can_list.append(self.map_edge[i])

        return edge_can_list

    def get_candidate_later(self, cur_point, last_point, last_edge, last_state, cnt):
        """
        :param cur_point: [px, py]
        :param last_point: [px, py]
        :param last_edge: MapEdge
        :param last_state: direction of vehicle in map edge
        :return: edge_can_list [edge0, edge1....]
        """
        edge_can_list = []
        T = 80 / 3.6 * 20   # dist_thread
        node_set = set()    # node_set用于判断是否访问过
        edge_set = set()    # edge_set用于记录能够访问到的边

        edge_set.add(last_edge.edge_index)

        q = Queue.PriorityQueue(maxsize=-1)  # 优先队列 best first search
        self.init_candidate_queue(last_point, last_edge, q, node_set)  # 搜索第一步，加入之前线段中的点

        while not q.empty():
            dnode = q.get()
            cur_node, cur_dist = dnode.node, dnode.dist
            if cur_dist >= T:  # 超过阈值后停止
                break
            for edge, node in cur_node.link_list:
                if node.nodeid in node_set:
                    continue
                node_set.add(node.nodeid)
                # 单行线需要判断角度
                edge_set.add(edge.edge_index)
                next_dnode = DistNode(node, cur_dist + edge.edge_length)
                node.prev_node = cur_node
                q.put(next_dnode)

        for i in edge_set:
            edge_can_list.append(self.map_edge[i])

        return edge_can_list

    def _get_mod_point_first(self, candidate, point):
        """
        :param candidate: 
        :param point: current point
        :return: project_point, sel_edge
        """
        min_dist, sel_edge = 1e20, None

        # first point
        for edge in candidate:
            # n0, n1 = edge.node0, edge.nodeid1
            p0, p1 = edge.node0.point, edge.node1.point
            dist = point2segment(point, p0, p1)
            if min_dist > dist:
                min_dist, sel_edge = dist, edge

        sel_node0, sel_node1 = sel_edge.node0, sel_edge.node1
        project_point, _, state = point_project(point, sel_node0.point, sel_node1.point)
        # print sel_edge.edge_index, min_dist
        return project_point, sel_edge, min_dist

    def _get_mod_point_later(self, candidate, point, last_point, cnt):
        """
        :param candidate: 
        :param point: current position point
        :param last_point: last position point
        :return: project_point, sel_edge, score
        """
        min_score, sel_edge, sel_angle = 1e10, None, 0
        min_dist = 1e10

        for edge in candidate:
            p0, p1 = edge.node0.point, edge.node1.point
            w0, w1 = 1.0, 10.0
            # 加权计算分数，考虑夹角的影响
            dist = point2segment(point, p0, p1)
            angle = calc_included_angle(last_point, point, p0, p1)
            if not edge.oneway and angle < 0:
                angle = -angle
            if angle < 0.3:
                continue
            score = w0 * dist + w1 * (1 - angle)
            # if cnt == 4:
            #     print edge.edge_index, dist, score, angle
            if score < min_score:
                min_score, sel_edge, min_dist, sel_angle = score, edge, dist, angle
        # print min_score, sel_edge.way_id, min_dist, sel_angle

        # if min_dist > 100:
        #     return None, None, 0
        if sel_edge is None:
            return None, None, 0
        project_point, _, state = point_project(point, sel_edge.node0.point, sel_edge.node1.point)
        if state == 1:
            # 点落在线段末端外
            project_point = sel_edge.node1.point
        elif state == -1:
            project_point = sel_edge.node0.point
        return project_point, sel_edge, min_score

    def get_mod_point(self, taxi_data, last_data, candidate, last_point, cnt=-1):
        """
        get best fit point matched with candidate edges
        :param taxi_data: Taxi_Data
        :param last_data: last Taxi_Data
        :param candidate: list[edge0, edge1, edge...]
        :param last_point: last position point ([px, py])
        :param cnt: for debug, int
        :return: matched point, matched edge, minimum distance from point to matched edge
        """
        point = [taxi_data.px, taxi_data.py]
        if last_point is None:
            # 第一个点
            try:
                last_point = [last_data.px, last_data.py]
            except AttributeError:
                last_point = None
        if last_point is None:
            return self._get_mod_point_first(candidate, point)
        else:
            return self._get_mod_point_later(candidate, point, last_point, cnt)

    def PNT_MATCH(self, data, last_data, last_edge, cnt=-1):
        """
        点到路段匹配，仅考虑前一个点
        :param data: 当前的TaxiData，见本模块
        :param last_data: 上一TaxiData数据
        :param last_edge: Edge 
        :param cnt:  for test, int
        :return: 本次匹配到的点 cur_point (Point) 本次匹配到的边 cur_edge (Edge)
        """
        first = False
        if last_data is None:
            cur_point, last_point = None, None
            first = True
        elif last_edge is None:
            first = True
            cur_point, last_point = None, None
        else:
            cur_point, last_point = [data.px, data.py], [last_data.px, last_data.py]
        # 用分块方法做速度更快
        # 实际上kdtree效果也不错，所以就用kdtree求最近节点knn
        if first:
            candidate_edges = self.get_candidate_first(data, self.kdt, self.X)
        else:
            candidate_edges = self.get_candidate_later(cur_point, last_point, last_edge, 0, cnt)
        # if cnt == 4:
        #     draw_edge_list(candidate_edges)

        cur_point, cur_edge, score = self.get_mod_point(data, last_data,
                                                        candidate_edges, last_point, cnt)
        # 注意：得分太高（太远、匹配不上）会过滤
        if score > 100:
            cur_point, cur_edge = None, None
        return cur_point, cur_edge

    def DYN_MATCH(self, traj_order):
        """
        using point match with dynamic programming, 
        :param traj_order: list of Taxi_Data 
        :return: 
        """
        first_point = True
        last_point, last_edge = None, None
        last_state = 0  # 判断双向道路当前是正向或者反向
        cnt = 0

        traj_mod = []  # 存放修正偏移后的data

        for data in traj_order:
            if first_point:
                candidate_edges = self.get_candidate_first(data, self.kdt, self.X)
                # Taxi_Data .px .py .stime .speed
                first_point = False
                mod_point, last_edge, score = self.get_mod_point(data, candidate_edges, last_point, cnt)
                state = 'c'
                data.set_edge([last_edge, score])
                traj_mod.append(data)
                last_point = mod_point
            else:
                # 首先判断两个点是否离得足够远
                T = 10000 / 3600 * 10
                cur_point = [data.px, data.py]
                interval = calc_dist(cur_point, last_point)
                # print cnt, interval
                if interval < T:
                    continue
                # 读取上一个匹配点的信息
                last_data = traj_mod[cnt - 1]
                last_point = [last_data.px, last_point.py]

                min_score, sel_edge, sel_score = 1e10, None, 0
                for last_edge, last_score in last_data.edge_info:
                    candidate_edges = self.get_candidate_later(cur_point, last_point, last_edge, last_state, cnt)

                    if len(candidate_edges) == 0:
                        # no match, restart
                        candidate_edges = self.get_candidate_first(data, self.kdt, self.X)
                        mod_point, cur_edge, score = self.get_mod_point(data, candidate_edges, None, cnt)
                        state = 'c'
                        cur_score = score + 1e5
                    else:
                        # if cnt == 147:
                        #     draw_edge_list(candidate_edges)
                        mod_point, cur_edge, score = self.get_mod_point(data, candidate_edges, last_point, cnt)
                        cur_score = score + last_score
                        state = 'r'
                    if cur_score < min_score:
                        min_score, sel_edge, sel_score = cur_score, cur_edge, score

                # if state == 'r':
                #     trace = get_trace(last_edge, cur_edge, last_point, mod_point)
                #     draw_seg(trace, 'b')

                offset_dist = calc_dist(mod_point, cur_point)
                if offset_dist > 100:
                    # 判断是否出现太远的情况
                    candidate_edges = self.get_candidate_first(data, self.kdt, self.X)
                    # draw_edge_list(candidate_edges)
                    mod_point, cur_edge = self.get_mod_point(data, candidate_edges, None, cnt)
                    state = 'm'

                traj_mod.append(data)
                last_point, last_edge = cur_point, cur_edge

            plt.text(data.px, data.py, '{0}'.format(cnt))
            plt.text(mod_point[0], mod_point[1], '{0}'.format(cnt), color=state)

            cnt += 1
            print cnt, data.px, data.py

        return traj_mod


