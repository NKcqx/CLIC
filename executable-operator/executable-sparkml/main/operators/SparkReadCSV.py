from pyspark.sql import SparkSession
import traceback
from model.OperatorBase import OperatorBase

"""
@ProjectName: CLIC
@Time       : 2020/11/26 17:45
@Author     : jimmy
@Description: 通过Spark读取CSV文件，并存为Dataframe形式
"""


class SparkOneHotEncode(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkOneHotEncode", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            sparkSession = self.getInputData("spark_Session")
            path = self.params("input_Path")
            header = self.params("header")              # 首行是否为表头
            inferSchema = self.params("inferSchema")    # 是否自动判断类型
            nanValue = self.params("nanValue")          # 空值

            self.setOutputData("result", SparkSession(sparkSession)
                               .read.csv(path=path, header=header, inferSchema=inferSchema, nanValue=nanValue))

        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())
