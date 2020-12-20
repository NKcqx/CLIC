from basic.PytorchOperatorFactory import PytorchOperatorFactory
from utils.TopoTraversal import TopoTraversal
import torch
import logging
import time
import random
import Executor

"""
@ProjectName: CLIC
@Time       : 2020/11/26 上午9:25
@Author     : zjchen
@Description: 
"""


def RandomID():
    return random.seed(time.process_time())


if __name__ == "__main__":
    start = time.process_time()
    print("Stage(Pytorch) ———— Start A New Pytorch Stage")

    # 初始化OptFactory
    Factory = PytorchOperatorFactory()

    # 手动初始化所有Operator
    # 根据path生成dataFrame
    source = Factory.createOperator("PdCsvSource", RandomID(), [], ["result"],
                                    {"input_Path": "/Users/zjchen/PycharmProjects/Pytorch/hotel_bookings/hotel_bookings.csv"})

    # 仅用于拼接dataFrame，如果需要索引请使用iloc算子

    concat = Factory.createOperator("PdConcat", RandomID(), ["data"], ["result"], {})

    # one-hot-encode

    dummies = Factory.createOperator("PdDummies", RandomID(), ["data"], ["result"], {"dummy_na": True})

    # 为所有Nan填0

    fillna = Factory.createOperator("PdFillNa", RandomID(), ["data"], ["result"], {"value": 0})

    # 索引算子

    iloc1 = Factory.createOperator("PdIloc", RandomID(), ["data"], ["result"], {"row_from": 0,
                                                                                "row_to": 100,
                                                                                "col_from": 0,
                                                                                "col_to": None})
    iloc2 = Factory.createOperator("PdIloc", RandomID(), ["data"], ["result"], {"row_from": None,
                                                                                "row_to": 500,
                                                                                "col_from": None,
                                                                                "col_to": -1})

    # 标准化

    standardization = Factory.createOperator("PdStandardization", RandomID(), ["data"], ["result"], {})

    # dataFrame->tensor

    tensorConverter = Factory.createOperator("TensorConverter", RandomID(), ["data"], ["result"], {"dtype": torch.float})

    # PCA算子

    PCA = Factory.createOperator("TorchPCA", RandomID(), ["data"], ["result"], {"k": 10,
                                                                                "center": True})

    # 手动构建DAG
    source.connectTo("result", iloc2, "data")
    iloc2.connectFrom("data", source, "result")

    iloc2.connectTo("result", standardization, "data")
    standardization.connectFrom("data", iloc2, "result")

    standardization.connectTo("result", fillna, "data")
    fillna.connectFrom("data", standardization, "result")

    fillna.connectTo("result", dummies, "data")
    dummies.connectFrom("data", fillna, "result")

    dummies.connectTo("result", tensorConverter, "data")
    tensorConverter.connectFrom("data", dummies, "result")

    tensorConverter.connectTo("result", PCA, "data")
    # iloc1.connectFrom("data", tensorConverter, "result")

    # iloc1.connectTo("result", PCA, "data")
    PCA.connectFrom("data", tensorConverter, "result")

    # 本来应该是由ArgsParser解析Yaml文件得到的，这里只有一个headNode
    headOperators = [source]

    Executor.execute(headOperators)




