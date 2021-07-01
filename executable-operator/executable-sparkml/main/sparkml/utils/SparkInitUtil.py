from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.conf import SparkConf

"""
@ProjectName: CLIC
@Time       : 2020/12/7 18:15
@Author     : Jimmy
@Description: 初始化spark需要的一些方法
"""


class SparkInitUtil:
    sparkSession = None
    sparkContext = None

    @staticmethod
    def __init__(appName="CLIC_SparkML_stage", master="local", conf=None):
        if conf is None:
            conf = SparkConf().setAppName(appName).setMaster(master)
        SparkInitUtil.sparkSession = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()
        SparkInitUtil.sparkContext = SparkContext.getOrCreate()

    @staticmethod
    def getDefaultSparkSession():
        if SparkInitUtil.sparkSession is None:
            # 本地测试采用local
            conf = SparkConf().setAppName("CLIC_SparkML_stage").setMaster("local")
            SparkInitUtil.sparkSession = SparkSession.builder \
                .config(conf=conf) \
                .getOrCreate()
        return SparkInitUtil.sparkSession

    @staticmethod
    def setSparkSession(conf):
        SparkInitUtil.sparkSession = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()
        SparkInitUtil.sparkContext = SparkContext.getOrCreate()

    @staticmethod
    def getDefaultSparkContext():
        if SparkInitUtil.sparkContext is None:
            # 本地测试采用local
            if SparkInitUtil.sparkSession is None:
                SparkInitUtil.getDefaultSparkSession()
            SparkInitUtil.sparkContext = SparkContext.getOrCreate()
        return SparkInitUtil.sparkContext
