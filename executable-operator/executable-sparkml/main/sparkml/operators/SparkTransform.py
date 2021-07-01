import traceback

from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger
import joblib

"""
@ProjectName: CLIC
@Time       : 2020/12/8 15:50
@Author     : Jimmy
@Description: Spark ML 拟合数据
"""

logger = Logger('OperatorLogger').logger


class SparkTransform(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkTransform", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            model = self.getInputData("sparkml.model")
            data = self.getInputData("data")

            self.setOutputData("result", model.transform(data))

        except Exception as e:
            logger.error(e.args)
            logger.error(traceback.format_exc())
