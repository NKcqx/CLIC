from pyspark import SparkConf

from executable.DagHook import DagHook
from sparkml.utils.SparkInitUtil import SparkInitUtil

"""
@ProjectName: CLIC
@Time       : 2021/6/24 9:59
@Author     : Jimmy
@Description: 
"""


class SparkMLDagHook(DagHook):
    def pre_handler(self, platformArgs):
        conf = SparkConf().setAppName("CLIC_SparkML_PCA") \
            .setMaster("local") \
            .set('spark.executor.memory', '8g') \
            .set('spark.driver.memory', '8g') \
            .set("spark.memory.fraction", 0.8) \
            .set("spark.sql.shuffle.partitions", "800") \
            .set("spark.memory.offHeap.enabled", "true") \
            .set("spark.memory.offHeap.size", "2g") \
            .set("spark.sql.debug.maxToStringFields", 1000)
        SparkInitUtil(conf=conf)

    def post_handler(self, platformArgs):
        ss = SparkInitUtil.getDefaultSparkSession()
        ss.stop()


