from pyspark.sql import SparkSession
import traceback
from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger
from sparkml.utils.SparkInitUtil import SparkInitUtil

"""
@ProjectName: CLIC
@Time       : 2020/11/26 17:45
@Author     : Jimmy
@Description: 读取CSV文件，并存为Dataframe形式
"""

logger = Logger('OperatorLogger').logger


class SparkReadCSV(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkReadCSV", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            sparkSession = SparkInitUtil.getDefaultSparkSession()
            path = self.params["inputPath"]
            header = True if str(self.params["header"]).lower() == 'true' else False                # 首行是否为表头
            inferSchema = True if str(self.params["inferSchema"]).lower() == 'true' else False     # 是否自动判断类型
            nanValue = self.params["nanValue"]     # 空值

            self.setOutputData("result", SparkSession(sparkSession)
                               .read.csv(path=path, header=header, inferSchema=inferSchema, nanValue=nanValue))

        except Exception as e:
            logger.error(e.args)
            logger.error(traceback.format_exc())
