# -*- coding: utf-8 -*-
from basics.AbstractOperator import AbstractOperatorImpl
import pandas as pd


class FileSourceImpl(AbstractOperatorImpl):
    '''
        argsList:argument dict 类型(参数名，参数值)
    '''

    def __init__(self, argsList):
        # toArchive: boolean
        self.toArchive = False  # 默认并不存储当前operator计算结果
        self.archiveFileName = ""

        self.dataStructure = None  # 存放数据的数据结构
        self.dataStructureType = None  # 数据结构的类型

        self.predecessorOperator = {}  # 存放 (参数类型，AbstractOperator) 前驱
        self.predecessorResult = {}  # 存放 (参数类型，前驱节点计算的结果)

        self.argsList = argsList  # 存放（参数名，参数值）

    def archive_operator_result(self):
        pass

    # 返回pandas dataframe
    def execute(self):
        self.dataStructureType = "dataframe"
        self.dataStructure = pd.read_csv(self.argsList["input"])
        return self.dataStructure
