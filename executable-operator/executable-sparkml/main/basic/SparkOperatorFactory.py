from model.OperatorFactory import OperatorFactory
from operators.CreateSparkSession import CreateSparkSession
from operators.DataframeColumns import DataframeColumns
from operators.DataframeDrop import DataframeDrop
from operators.DataframeLimit import DataframeLimit
from operators.DataframeUnion import DataframeUnion
from operators.SparkLinearRegression import SparkLinearRegression
from operators.SparkOneHotEncode import SparkOneHotEncode
from operators.SparkFillNa import SparkFillNa
from operators.SparkPCA import SparkPCA
from operators.SparkReadCSV import SparkReadCSV
from operators.SparkStandardScaler import SparkStandardScaler

"""
@ProjectName: CLIC
@Time       : 2020/11/30 10:18
@Author     : jimmy
@Description: 
"""


class SparkOperatorFactory(OperatorFactory):
    def __init__(self):
        self.operatorMap = {
            "CreateSparkSession": CreateSparkSession,
            "DataframeColumns": DataframeColumns,
            "DataframeDrop": DataframeDrop,
            "DataframeLimit": DataframeLimit,
            "DataframeUnion": DataframeUnion,
            "SparkLinearRegression": SparkLinearRegression,
            "SparkOneHotEncode": SparkOneHotEncode,
            "SparkFillNa": SparkFillNa,
            "SparkPCA": SparkPCA,
            "SparkReadCSV": SparkReadCSV,
            "SparkStandardScaler": SparkStandardScaler
        }

    def createOperator(self, name, id, inputKeys, outputKeys, params):
        if name not in self.operatorMap.keys():
            raise ValueError("操作符不存在或未被初始化！")
        optCls = self.operatorMap[name]
        return optCls(id, inputKeys, outputKeys, params)
