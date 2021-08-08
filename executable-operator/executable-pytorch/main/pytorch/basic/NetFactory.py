import torch

"""
Time       : 2021/7/20 3:41 下午
Author     : zjchen
Description:
"""


class NetFactory(object):
    def __init__(self):
        self.netMap = {
            # TODO：实现几个内建的Net结构
        }

    def createNet(self, name):
        if name not in self.netMap.keys():
            raise ValueError("Net不存在或未被初始化！")
        net = self.netMap[name]
        return net
