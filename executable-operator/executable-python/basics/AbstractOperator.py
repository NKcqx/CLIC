# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractclassmethod


class AbstractOperatorImpl(metaclass=ABCMeta):
    # def __init__(self):
    #     # toArchive: boolean
    #     self.toArchive = False  # 默认并不存储当前operator计算结果
    #     self.archiveFileName = ""
    #     self.dataStructure = None  # 存放数据的数据结构
    #     self.dataStructureType = None  # 数据结构的类型
    #     self.predecessorOperator = {}  # 存放 (参数类型，AbstractOperator) 前驱
    #     self.predecessorResult = {}  # 存放 (参数类型，前驱节点计算的结果)
    #     self.argsList = {}  # 存放（参数名，参数值）

    @abstractclassmethod
    def archive_operator_result(cls):
        pass

    @abstractclassmethod
    def execute(cls):
        pass
