from model.OperatorFactory import OperatorFactory
from operators.DataframeColumns import DataframeColumns
from operators.DataframeDrop import DataframeDrop
from operators.DataframeFillNa import DataframeFillNa
from operators.DataframeFilter import DataframeFilter
from operators.DataframeLimit import DataframeLimit
from operators.DataframeUnion import DataframeUnion
from operators.DataframeWithColumn import DataframeWithColumn
from operators.SparkLinearRegression import SparkLinearRegression
from operators.SparkOneHotEncode import SparkOneHotEncode
from operators.SparkCountVectorizer import SparkCountVectorizer
from operators.SparkLDA import SparkLDA
from operators.SparkPCA import SparkPCA
from operators.SparkReadCSV import SparkReadCSV
from operators.SparkRegexTokenizer import SparkRegexTokenizer
from operators.SparkStandardScaler import SparkStandardScaler
from operators.SparkStopWordsRemover import SparkStopWordsRemover
from operators.SparkTransform import SparkTransform
from operators.SparkVectorAssembler import SparkVectorAssembler

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
            "DataframeFilter": DataframeFilter,
            "DataframeLimit": DataframeLimit,
            "DataframeUnion": DataframeUnion,
            "DataframeWithColumn": DataframeWithColumn,
            "SparkCountVectorizer": SparkCountVectorizer,
            "SparkLinearRegression": SparkLinearRegression,
            "SparkOneHotEncode": SparkOneHotEncode,
            "SparkLDA": SparkLDA,
            "SparkPCA": SparkPCA,
            "SparkReadCSV": SparkReadCSV,
            "SparkRegexTokenizer": SparkRegexTokenizer,
            "SparkStandardScaler": SparkStandardScaler,
            "SparkStopWordsRemover": SparkStopWordsRemover,
            "SparkTransform": SparkTransform,
            "SparkVectorAssembler": SparkVectorAssembler
        }

    def createOperator(self, name, id, inputKeys, outputKeys, params):
        if name not in self.operatorMap.keys():
            raise ValueError("操作符不存在或未被初始化！")
        optCls = self.operatorMap[name]
        return optCls(id, inputKeys, outputKeys, params)
