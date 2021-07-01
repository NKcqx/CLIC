import traceback

from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger
from pyspark.sql.functions import *

"""
@ProjectName: CLIC
@Time       : 2020/12/9 15:01
@Author     : Jimmy
@Description: 
"""

logger = Logger('OperatorLogger').logger


class DataframeWithColumn(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("DataframeWithColumn", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            col_name = self.params["colName"]
            col = self.params["col"]

            self.setOutputData("result", df.withColumn(col_name, eval(col)))

        except Exception as e:
            logger.error(e.args)
            logger.error(traceback.format_exc())
