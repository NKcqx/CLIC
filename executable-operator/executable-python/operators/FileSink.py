# -*- coding: utf-8 -*-
from basics.AbstractOperator import AbstractOperatorImpl


class FileSinkImpl(AbstractOperatorImpl):
    '''
        argsList :argument dict 类型("output"，outputFileName)          非前驱节点输入参数
        inputOperators :argument  inputOperator
    '''

    def __init__(self, argsList, inputOperators):
        # toArchive: boolean
        self.toArchive = False  # 默认并不存储当前operator计算结果
        self.archiveFileName = ""

        self.dataStructure = None  # 存放数据的数据结构
        self.dataStructureType = None  # 数据结构的类型

        self.predecessorOperator = inputOperators  # 存放 (参数名，AbstractOperator) 前驱
        self.predecessorResult = None  # 存放 (参数名称，前驱节点计算的结果)

        self.argsList = argsList  # 存放（参数名，参数值）

    def archive_operator_result(self):
        pass

    # 返回pandas dataframe
    def execute(self):
        # 执行所有前置算子
        self.execute_predecessor()

        # 获取上游节点的输出，作为本节点的输入
        df = self.predecessorResult  # input for FileSink Operator

        # 写csv 文件
        return df.to_csv(self.argsList["output"])

    def execute_predecessor(self):
        self.predecessorResult = self.predecessorOperator.execute()
