from model.OperatorFactory import OperatorFactory
from operators.DataframeColumns import DataframeColumns
from operators.DataframeDrop import DataframeDrop
from operators.DataframeLimit import DataframeLimit
from operators.DataframeUnion import DataframeUnion
from operators.SparkLinearRegression import SparkLinearRegression
from operators.SparkOneHotEncode import SparkOneHotEncode
from operators.DataframeFillNa import DataframeFillNa
from operators.SparkCountVectorizer import SparkCountVectorizer
from operators.SparkLDA import SparkLDA
from operators.SparkPCA import SparkPCA
from operators.SparkReadCSV import SparkReadCSV
from operators.SparkRegexTokenizer import SparkRegexTokenizer
from operators.SparkStandardScaler import SparkStandardScaler
from operators.SparkStopWordsRemover import SparkStopWordsRemover

"""
@ProjectName: CLIC
@Time       : 2020/11/30 10:18
@Author     : jimmy
@Description: 
"""


class SparkOperatorFactory(OperatorFactory):
    def __init__(self):
        self.operatorMap = {
            "DataframeColumns": DataframeColumns,
            "DataframeDrop": DataframeDrop,
            "DataframeFillNa": DataframeFillNa,
            "DataframeLimit": DataframeLimit,
            "DataframeUnion": DataframeUnion,
            "SparkCountVectorizer": SparkCountVectorizer,
            "SparkLinearRegression": SparkLinearRegression,
            "SparkOneHotEncode": SparkOneHotEncode,
            "SparkLDA": SparkLDA,
            "SparkPCA": SparkPCA,
            "SparkReadCSV": SparkReadCSV,
            "SparkRegexTokenizer": SparkRegexTokenizer,
            "SparkStandardScaler": SparkStandardScaler,
            "SparkStopWordsRemover": SparkStopWordsRemover
        }

    def createOperator(self, name, id, inputKeys, outputKeys, params):
        if name not in self.operatorMap.keys():
            raise ValueError("操作符不存在或未被初始化！")
        optCls = self.operatorMap[name]
        return optCls(id, inputKeys, outputKeys, params)
