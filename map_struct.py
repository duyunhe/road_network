# -*- coding: utf-8 -*-
# @Time    : 2018/9/10 10:55
# @Author  : 
# @简介    : 道路数据结构
# @File    : map_struct.py

import math


def segment2point(seg_list):
    """
    :param seg_list: list of Segment
    :return: 
    """
    point_list = [seg_list[0].begin_point]
    for seg in seg_list:
        point_list.append(seg.end_point)
    return point_list


class Vector:
    """
    向量
    """
    def __init__(self, px, py):
        self.px, self.py = px, py

    def __neg__(self):
        return Vector(-self.px, -self.py)


class Point:
    """
    点，px py  
    """
    def __init__(self, px, py):
        self.px, self.py = px, py
        self.cross = 0      # 是否为交点   1 进路口 2 出路口
        self.cross_name = ""
        self.cross_seg = -1             # 相交时本线段的序号
        self.cross_other_seg = -1       # 对面相交线段的序号

    def __eq__(self, other):
        return math.fabs(self.px - other.px) < 1e-5 and math.fabs(self.py - other.py) < 1e-5


class Segment:
    """
    道路中的线段，
    有方向，SegmentID，begin_point, end_point, road_name
    """
    def __init__(self, begin_point=None, end_point=None, name='', sid=0):
        self.begin_point, self.end_point = begin_point, end_point
        self.name, self.sid = name, sid
        self.entrance, self.exit = None, None

    def set_invert(self):
        self.begin_point, self.end_point = self.end_point, self.begin_point

    def set_sid(self, sid):
        self.sid = sid

    def set_name(self, name):
        self.name = name

    def add_entrance(self, point):
        """
        添加一个路口的进路口点
        :param point: Point
        :return: 
        """
        self.entrance = point

    def add_exit(self, point):
        self.exit = point


class Road:
    """
    道路
    字段：道路名，方向，道路中线段list  
    """
    def __init__(self, name, ort, rid):
        self.name, self.ort = name, ort
        self.seg_list = []              # list of Segment
        self.point_list = []            # list of Point
        self.rid = rid
        self.mark = 0
        self.grid_set = None

    def set_rid(self, rid):
        self.rid = rid

    def set_grid_set(self, gs):
        self.grid_set = gs

    def set_mark(self, mark):
        self.mark = mark

    def add_point(self, point):
        self.point_list.append(point)

    def set_point_list(self, point_list):
        self.point_list = point_list

    def gene_segment(self):
        self.seg_list = []
        last_point = None
        sid = 0
        for point in self.point_list:
            if last_point is not None:
                seg = Segment(last_point, point, self.name, sid)
                self.seg_list.append(seg)
                sid += 1
            last_point = point

    def add_segment(self, segment):
        """
        添加线段
        :param segment: Segment
        :return: 
        """
        self.seg_list.append(segment)

    def get_entrance(self):
        e_list = []
        for seg in self.seg_list:
            if seg.entrance is not None:
                e_list.append(seg.entrance)
        return e_list

    def get_exit(self):
        e_list = []
        for seg in self.seg_list:
            if seg.exit is not None:
                e_list.append(seg.exit)
        return e_list

    def get_name(self):
        return self.name

    def get_path(self):
        return segment2point(self.seg_list)

    def get_path_without_crossing(self):
        """
        对外接口
        路口线段隐藏，得到新的list
        由于整条完整的道路被路口分开，因此存在多个list，每个list代表一条被隔开的道路list
        :return: path_list
        """
        path_list, temp_seg_list = [], []
        # path_list : list of path
        # path: list of Point
        # temp_seg_list: temp segment list
        cross = False
        for seg in self.seg_list:
            if seg.entrance is None and seg.exit is not None:
                cross = True
                break
            if seg.entrance is not None and seg.exit is None:
                break

        for i, seg in enumerate(self.seg_list):
            if seg.entrance is None and seg.exit is None:
                if not cross:
                    temp_seg_list.append(seg)
            elif seg.entrance is not None and seg.exit is not None:
                # 在正常情况下不会出现，因为道路的交叉点会将segment分开
                # 不会出现同一条segment里面既有路口一侧又有路口另一侧的情况
                # 测试各种情况用
                if not cross:
                    s0 = Segment(seg.begin_point, seg.entrance)
                    temp_seg_list.append(s0)
                    s1 = Segment(seg.exit, seg.end_point)
                    path_list.append(segment2point(temp_seg_list))
                    temp_seg_list = [s1]
                else:
                    s0 = Segment(seg.exit, seg.entrance)
                    temp_seg_list = [s0]
                    path_list.append(segment2point(temp_seg_list))
                    temp_seg_list = []
            elif seg.entrance is not None and seg.exit is None:
                s0 = Segment(seg.begin_point, seg.entrance)
                temp_seg_list.append(s0)
                path_list.append(segment2point(temp_seg_list))
                temp_seg_list = []
                cross = True
            elif seg.entrance is None and seg.exit is not None:
                s1 = Segment(seg.exit, seg.end_point)
                temp_seg_list.append(s1)
                cross = False
        if len(temp_seg_list) != 0:
            path_list.append(segment2point(temp_seg_list))
        return path_list


class DistNode(object):
    def __init__(self, node, dist):
        self.node = node
        self.dist = dist

    def __lt__(self, other):
        return self.dist < other.dist


class MapRoad(object):
    def __init__(self, road_name, direction, level):
        self.road_name, self.direction, self.lev = road_name, direction, level
        self.node_list = []         # 代表道路中线段的序列

    def add_node(self, map_node):
        self.node_list.append(map_node)


class MapNode(object):
    """
    点表示
    point([px,py]), nodeid, link_list, rlink_list, dist_dict
    在全局维护dict, key=nodeid, value=MapNode
    """
    def __init__(self, point, nodeid):
        self.point, self.nodeid = point, nodeid
        self.link_list = []         # 连接到其他点的列表, [[edge0, node0], [edge1, node1]....]
        self.rlink_list = []
        self.prev_node = None       # bfs时寻找路径, MapNode
        self.prev_edge = None

    def add_link(self, edge, node):
        self.link_list.append([edge, node])

    def add_rlink(self, edge, node):
        self.rlink_list.append([edge, node])


class MapEdge(object):
    """
    线段表示
    node0(MapNode), node1,
    oneway(true or false), edge_index, edge_length
    维护list[MapEdge]
    """
    def __init__(self, node0, node1, oneway, edge_index, edge_length, way_id):
        self.node0, self.node1 = node0, node1
        self.oneway = oneway
        self.edge_index = edge_index
        self.edge_length = edge_length
        self.way_id = way_id


class MatchResult(object):
    """
    匹配结果
    current point
    match_list: [MatchList, ...]
    """
    class MatchPoint(object):
        """
        edge_index, match_point, [last_index1, last_index2...], dist, score
        """
        def __init__(self, edge_index, mod_point, last_index_list, dist, score):
            self.edge_index, self.last_index_list = edge_index, last_index_list
            self.mod_point = mod_point
            self.dist, self.score = dist, score

    def __init__(self, point):
        self.point, self.first = point, True
        self.match_point_list = []
        self.sel = -1

    def add_match(self, edge_index, mod_point, index_list, dist, score):
        mp = self.MatchPoint(edge_index, mod_point, index_list, dist, score)
        self.match_point_list.append(mp)

    def set_sel(self, sel):
        self.sel = sel

    def set_first(self, first):
        self.first = first
