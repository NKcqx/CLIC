from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.conf import SparkConf

"""
@ProjectName: CLIC
@Time       : 2020/12/7 18:15
@Author     : jimmy
@Description: 初始化spark需要的一些方法
"""


class SparkInitUtil:
    sparkSession = None
    sparkContext = None

    @staticmethod
    def __init__(conf=None):
        if conf is None:
            conf = SparkConf().setAppName("CLIC_Demo").setMaster("local")
        SparkInitUtil.sparkSession = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()
        SparkInitUtil.sparkContext = SparkContext.getOrCreate()

    @staticmethod
    def getSparkSession():
        return SparkInitUtil.sparkSession